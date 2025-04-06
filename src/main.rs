use anyhow;
use chrono::{DateTime, Utc};
use clap::{arg, ArgMatches, Command, Parser};
use rusqlite::{Connection, Result};
use serenity::all::MessagePagination;
use serenity::http::{Http, HttpBuilder};
use serenity::model::channel::Message;
use serenity::model::id::{ChannelId, GuildId};
use std::env;
use charming::{ImageRenderer, ImageFormat};
use charming::{component::{
    Axis, DataZoom, DataZoomType, Feature, Restore, SaveAsImage, Title, Toolbox,
    ToolboxDataZoom,
}, element::{AreaStyle, AxisType, Color, ColorStop, LineStyle, Symbol, Tooltip, Trigger}, series::Line, Chart, HtmlRenderer};
use chrono::{Days, NaiveDate};

fn cli() -> Command {
    Command::new("statcat")
        .about("A discord statistics experience")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(
            Command::new("gather")
                .about("Gathers messages")
                .arg(arg!(<GUILD_ID> "The guild to gather messages from"))
                .arg_required_else_help(true),
        )
    .subcommand(
            Command::new("word")
                .about("Gets word statistics")
                .arg(arg!(<GUILD_ID> "The guild to check word statistics on"))
                .arg(arg!(<WORD> "The word or phrase to grab statistics of"))
                .arg_required_else_help(true),
        )
}

struct StatcatMessage {
    message_id: u64,
    channel_id: u64,
    guild_id: u64,
    author_id: u64,
    author_name: String,
    bot: bool,
    timestamp: DateTime<Utc>,
    content: String,
}

async fn get_channel_messages_paginated(
    client: &Http,
    channel_id: ChannelId,
) -> anyhow::Result<Vec<Message>> {
    // Get most recent message
    let mut messages: Vec<Message> = client.get_messages(channel_id, None, Some(1)).await?;

    if messages.is_empty() {
        return Ok(messages);
    }

    let mut total_messages = 0;

    // Iterate while chunks are Ok, and then break if chunk is empty
    while let Ok(mut message_chunk) = client
        .get_messages(
            channel_id,
            Some(MessagePagination::Before(messages.last().unwrap().id)),
            Some(100),
        )
        .await
    {
        if message_chunk.is_empty() {
            break;
        }

        total_messages += message_chunk.len();
        messages.append(&mut message_chunk);
        println!("\r\rCollected {} messages from the channel", total_messages);
    }

    Ok(messages)
}

fn insert_channel_messages(
    db_connection: &mut Connection,
    guild_id: GuildId,
    channel_id: ChannelId,
    messages: &Vec<Message>,
) -> Result<(), rusqlite::Error> {
    let tx = db_connection.transaction()?;
    if let Ok(mut stmt) = tx.prepare(
        "INSERT or IGNORE INTO messages (\
                message_id, \
                channel_id, \
                guild_id, \
                author_id, \
                author_name, \
                bot, \
                timestamp, \
                content \
            ) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
    ) {
        for message in messages {
            stmt.execute((
                message.id.get(),
                channel_id.get(),
                guild_id.get(),
                message.author.id.get(),
                &message.author.name,
                message.author.bot,
                message.timestamp.timestamp(),
                &message.content,
            ))?;
        }
    }

    tx.commit()?;

    Ok(())
}

fn get_all_messages(db_connection: &mut Connection, guild_id: GuildId) -> Vec<StatcatMessage> {
    let mut stmt = db_connection.prepare("SELECT * FROM messages").unwrap();

    let mut rows = stmt
        .query_map([], |row| {
            Ok(StatcatMessage {
                message_id: row.get(0)?,
                channel_id: row.get(1)?,
                guild_id: row.get(2)?,
                author_id: row.get(3)?,
                author_name: row.get(4)?,
                bot: row.get(5)?,
                timestamp: DateTime::from_timestamp(row.get(6)?, 0).unwrap(),
                content: row.get(7)?,
            })
        })
        .unwrap();

    rows.map(|message| message.unwrap()).collect()
}

async fn gather(db_connection: &mut Connection, discord_client: &Http, guild_id: GuildId) -> anyhow::Result<()>{
        let guild_id = GuildId::new(644752766241341460);
        for (channel_id, guild_channel) in guild_id.channels(&discord_client).await? {
        // Try to fetch messages from channels that have no messages
        let num_channel_messages = db_connection.query_row::<u32, _, _>(
            "SELECT count(*) FROM messages WHERE channel_id = (?1)",
            [channel_id.get()],
            |row| row.get(0),
        )?;
        if num_channel_messages != 0 {
            continue;
        }

        // Fetch messages!
        println!("Fetching messages from {}", guild_channel.name);
        let messages = get_channel_messages_paginated(&discord_client, channel_id).await?;
        insert_channel_messages(db_connection, guild_id, channel_id, &messages)?;
        println!(
            "Got {} messages from {}",
            &messages.len(),
            channel_id.name(&discord_client).await?
        );
    }

    Ok(())
}

#[derive(Debug)]
struct StatcatWordViewMessage {
    date: String,
    count: i64
}

fn word_chart(db_connection: &mut Connection, guild_id: GuildId, word: String) -> anyhow::Result<()> {
    // let mut dates = Vec::new();
    // let mut values = Vec::new();

    db_connection.execute(
        &format!("CREATE TEMP VIEW word_view
            AS
            SELECT
                STRFTIME('%Y-%V', DATE(timestamp, 'unixepoch')) AS Date,
                COUNT(message_id)
            from messages
            WHERE content LIKE '%{}%'
            GROUP BY Date", word),
        []
    ).expect("Unable to set up the temporary view.");

    let mut stmt = db_connection.prepare("SELECT * FROM word_view").unwrap();

    let mut rows = stmt
        .query_map([], |row| {
            Ok(StatcatWordViewMessage {
                date: row.get(0)?,
                count: row.get(1)?,
            })
        })?;

    let mut dates = vec![];
    let mut values = vec![];
    while let Some(Ok(row)) = rows.next() {
        dates.push(row.date);
        values.push(row.count);
    }

    let chart = Chart::new()
            .tooltip(Tooltip::new().trigger(Trigger::Axis))
            .title(Title::new().left("center").text(format!("Messages containing the word {}", word)))
            .toolbox(
                Toolbox::new().feature(
                    Feature::new()
                        .data_zoom(ToolboxDataZoom::new().y_axis_index("none"))
                        .restore(Restore::new())
                        .save_as_image(SaveAsImage::new()),
                ),
            )
            .x_axis(
                Axis::new()
                    .type_(AxisType::Category)
                    .boundary_gap(false)
                    .data(dates),
            )
            .y_axis(Axis::new().type_(AxisType::Value))
            .data_zoom(DataZoom::new().type_(DataZoomType::Inside).start(0).end(10))
            .data_zoom(DataZoom::new().start(0).end(10))
            .series(
                Line::new()
                    .symbol(Symbol::None)
                    .line_style(LineStyle::new().color("rgb(255, 70, 131"))
                    .area_style(AreaStyle::new().color(Color::LinearGradient {
                        x: 0.,
                        y: 0.,
                        x2: 0.,
                        y2: 1.,
                        color_stops: vec![
                        ColorStop::new(0, "rgb(255, 158, 68)"),
                        ColorStop::new(1, "rgb(255, 70, 131)"),
                    ],
                }))
                .data(values),
        );

    // Chart dimension 1000x800.
    let mut renderer = HtmlRenderer::new("my charts", 1000, 800);
    renderer.save(&chart, "./charts/chart.html")?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Match on the CLI arguments
    let matches = cli().get_matches();

    // Set up sqlite connection, creates a database if it doesn't exist
    let mut db_connection = Connection::open("statcat.db")?;
    db_connection.execute(
        "create table if not exists messages (
             message_id integer primary key,
             channel_id integer not null,
             guild_id integer not null,
             author_id integer not null,
             author_name text not null,
             bot bool not null,
             timestamp datetime not null,
             content text
         )",
        [],
    )
    .expect("Unable to set up the database.");

    let discord_token =
        env::var("DISCORD_TOKEN").expect("Expected a discord token in the environment");
    let http_client = HttpBuilder::new(discord_token).build();

    match matches.subcommand() {
        Some(("gather", sub_matches)) => {
            gather(&mut db_connection, &http_client, GuildId::new(sub_matches.get_one::<String>("GUILD_ID").expect("Expected a guild id").to_owned().parse()?)).await
        }
        Some(("word", sub_matches)) => {
            word_chart(&mut db_connection, GuildId::new(sub_matches.get_one::<String>("GUILD_ID").expect("Expected a guild id").to_owned().parse()?), sub_matches.get_one::<String>("WORD").expect("Expected a word").to_string())
        }
        _ => Ok(())
    }
}
