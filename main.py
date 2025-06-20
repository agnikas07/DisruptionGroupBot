import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import Modal, TextInput
import os
import gspread
from dotenv import load_dotenv
import datetime
import pandas as pd
import calendar

import pytz

# --- CONFIGURATION ---
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
POSTING_CHANNEL_ID = os.getenv('POSTING_CHANNEL_ID')
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
GOOGLE_SPREADSHEET_NAME = os.getenv('GOOGLE_SPREADSHEET_NAME')
GOOGLE_WORKSHEET_NAME = os.getenv('GOOGLE_WORKSHEET_NAME')


# --- Google Sheets Setup ---
try:    
    gc = gspread.service_account(filename=GOOGLE_SERVICE_ACCOUNT_FILE)
    sh = gc.open(GOOGLE_SPREADSHEET_NAME)
    worksheet = sh.worksheet(GOOGLE_WORKSHEET_NAME)
    print("Google Sheets connected successfully.")
except gspread.exceptions.SpreadsheetNotFound:
    print("Error: Google Sheets spreadsheet not found. Please check the name and make sure that the spreadsheet is shared with the bot's service account email.")
    exit()
except FileNotFoundError:
    print(f"Error: Google Sheets credentials file '{GOOGLE_SERVICE_ACCOUNT_FILE}' not found. Please ensure the file is in the correct location.")
    exit()
except Exception as e:
    print(f"An unexpected error occurred while connecting to Google Sheets: {e}")
    exit()


# --- BOT SETUP ---
intents = discord.Intents.default()
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


# --- HELPER FUNCTIONS ---
def get_leaderboard_data(period: str) -> pd.DataFrame:
    """
    Fetches all sales data from the Google Sheet, processes it, and returns a
    sorted leaderboard DataFrame based on the specified period.
    """
    try:
        records = worksheet.get_all_records()
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)

        required_cols = ['Date', 'User ID', 'Name', 'Premium']
        if not all(col in df.columns for col in required_cols):
            print(f"Error: Sheet is missing one of the required columns: {required_cols}")
            return pd.DataFrame()
        
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Premium'] = pd.to_numeric(df['Premium'], errors='coerce')
        df['User ID'] = df['User ID'].astype(str) 
        df.dropna(subset=['Date', 'Premium', 'User ID'], inplace=True)
        df = df[df['User ID'] != '']

        now = pd.Timestamp.now(tz='UTC').tz_convert(None)

        if period == 'today':
            start_date = now.normalize()
            df_filtered = df[df['Date'] >= start_date]
        elif period == 'week':
            start_date = (now - pd.Timedelta(days=now.weekday())).normalize()
            df_filtered = df[df['Date'] >= start_date]
        elif period == 'month':
            start_date = now.replace(day=1).normalize()
            df_filtered = df[df['Date'] >= start_date]
        else:
            df_filtered = df

        if df_filtered.empty:
            return pd.DataFrame()
        
        leaderboard = df_filtered.groupby(['User ID', 'Name']).agg(
            TotalPremium=('Premium', 'sum'),
            SaleCount=('Premium', 'count'),
        ).reset_index()

        leaderboard_sorted = leaderboard.sort_values('TotalPremium', ascending=False).head(10)

        leaderboard_sorted.reset_index(drop=True, inplace=True)
        leaderboard_sorted.index += 1

        return leaderboard_sorted
    
    except Exception as e:
        print(f"An error occured in get_leaderboard_data: {e}")
        return pd.DataFrame()
    

def format_leaderboard_section(title: str, leaderboard_df: pd.DataFrame) -> str:
    """
    Formats a leaderboard DataFrame into a string for Discord.
    """
    if leaderboard_df.empty:
        return f"**{title}**\n*No entries yet for this period.*"
    
    lines = [f"**{title}**"]
    for rank, row in leaderboard_df.iterrows():
        user_id_clean = str(row['User ID']).split('.')[0]
        user_mention = f"<@{user_id_clean}>"
        premium_formatted = f"${row['TotalPremium']:,.2f}"
        sale_count = int(row['SaleCount'])
        lines.append(f"{rank}. {user_mention}: {premium_formatted} | {sale_count} FP")

    return "\n".join(lines)


def format_top_performer_section(month_df: pd.DataFrame) -> str:
    """
    Creates the 'Top Performer' section with month-end projection.
    """
    if month_df.empty:
        return ""
    
    top_performer = month_df.iloc[0]

    now = datetime.datetime.now()
    days_in_month = calendar.monthrange(now.year, now.month)[1]
    current_day = now.day

    if current_day >= days_in_month:
        projection_factor = 1.0
    else:
        projection_factor = days_in_month / current_day

    user_id_clean = str(top_performer['User ID']).split('.')[0]
    user_mention = f"<@{user_id_clean}>"
    current_premium = top_performer['TotalPremium']
    current_sales = top_performer['SaleCount']

    projected_premium = current_premium * projection_factor
    projected_sales = round(current_sales * projection_factor)

    title = f"🏆 **Top Performer:** {user_mention}"
    current_line = f"Current: ${current_premium:,.2f} | {int(current_sales)} FP"
    projected_line = f"Projected: ${projected_premium:,.2f} | {projected_sales} FP"

    return f"{title}\n{current_line}\n{projected_line}"


def format_rising_star_section() -> str:
    """
    Finds the user with the highest percentage sales increase from last week to this week,
    only considering users who made sales in BOTH weeks.
    """
    try:
        records = worksheet.get_all_records()
        if not records:
            return ""

        df = pd.DataFrame(records)
        required_cols = ['Date', 'User ID', 'Name', 'Premium']
        if not all(col in df.columns for col in required_cols):
            return ""
        
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Premium'] = pd.to_numeric(df['Premium'], errors='coerce')
        df['User ID'] = df['User ID'].astype(str)
        df.dropna(subset=['Date', 'Premium', 'User ID'], inplace=True)

        now = pd.Timestamp.now(tz='UTC').tz_convert(None)
        start_of_this_week = (now - pd.Timedelta(days=now.weekday())).normalize()
        start_of_last_week = start_of_this_week - pd.Timedelta(weeks=1)

        df_this_week = df[df['Date'] >= start_of_this_week]
        df_last_week = df[(df['Date'] >= start_of_last_week) & (df['Date'] < start_of_this_week)]

        sales_this_week = df_this_week.groupby('User ID')['Premium'].sum().reset_index()
        sales_last_week = df_last_week.groupby('User ID')['Premium'].sum().reset_index()

        if sales_last_week.empty:
            return ""

        merged = pd.merge(sales_this_week, sales_last_week, on='User ID', how='inner', suffixes=('_this', '_last'))
        
        if merged.empty:
            return ""

        merged['Growth'] = ((merged['Premium_this'] - merged['Premium_last']) / merged['Premium_last']) * 100
        
        rising_stars = merged.sort_values(by='Growth', ascending=False)
        
        if rising_stars.empty or rising_stars.iloc[0]['Growth'] <= 0:
            return ""

        top_riser_row = rising_stars.iloc[[0]] 

        user_info = df[['User ID', 'Name']].drop_duplicates(subset=['User ID'])
        top_riser_with_name = pd.merge(top_riser_row, user_info, on='User ID', how='left')

        if top_riser_with_name.empty:
            return ""
        
        top_riser = top_riser_with_name.iloc[0]
        
        user_mention = f"<@{str(top_riser['User ID']).split('.')[0]}>"
        this_week_total = top_riser['Premium_this']
        last_week_total = top_riser['Premium_last']
        growth_percent = top_riser['Growth']
        
        title = f"🌟 **Rising Star:** {user_mention}"
        line1 = f"${this_week_total:,.2f} this week (↑{growth_percent:.0f}% from last week)"
        line2 = f"Last week: ${last_week_total:,.2f}"

        return f"{title}\n{line1}\n{line2}"

    except Exception as e:
        print(f"An error occurred in format_rising_star_section: {e}")
        return ""
    

def get_full_leaderboard_content() -> str:
    """Generates the string content for the full Friday leaderboard."""
    today_df = get_leaderboard_data('today')
    week_df = get_leaderboard_data('week')
    month_df = get_leaderboard_data('month')

    top_performer_content = format_top_performer_section(month_df)
    rising_star_content = format_rising_star_section()

    day_of_week = datetime.datetime.now().strftime('%A')
    today_title = f"📊 Today ({day_of_week}):"
    today_content = format_leaderboard_section(today_title, today_df)

    week_title = "📅 Week-to-Date:"
    week_content = format_leaderboard_section(week_title, week_df)

    month_title = "🥇 Month-to-Date:"
    month_content = format_leaderboard_section(month_title, month_df)

    content_sections = []
    if top_performer_content: 
        content_sections.append(top_performer_content)
    content_sections.append(today_content)
    content_sections.append(week_content)
    content_sections.append(month_content)
    if rising_star_content:
        content_sections.append(rising_star_content)

    return "\n\n".join(content_sections)


def get_daily_leaderboard_content() -> str:
    """Generates the string content for the standard daily leaderboard."""
    today_df = get_leaderboard_data('today')
    week_df = get_leaderboard_data('week')
    month_df = get_leaderboard_data('month')

    day_of_week = datetime.datetime.now().strftime('%A')
    today_title = f"📊 Today ({day_of_week}):"
    today_content = format_leaderboard_section(today_title, today_df)

    week_title = "📅 Week-to-Date:"
    week_content = format_leaderboard_section(week_title, week_df)

    month_title = "🥇 Month-to-Date:"
    month_content = format_leaderboard_section(month_title, month_df)

    return f"{today_content}\n\n{week_content}\n\n{month_content}"


# --- MODAL DEFINITION ---
class SaleEntryModal(Modal, title='Enter Sale Details'):
    premium = TextInput(label='Annual Premium Amount', placeholder='e.g., 1250.75', required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        submission_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user_id = interaction.user.id
        discord_username = interaction.user.display_name
        premium_amount_str = self.premium.value
        try:
            unrounded_premium = float(premium_amount_str.replace(',', ''))
            premium_amount = round(unrounded_premium, 2)
        except ValueError:
            await interaction.followup.send("❌ **Error:** Please enter a valid number.", ephemeral=True)
            return
        try:
            row_to_add = [submission_date, str(user_id), discord_username, premium_amount]
            worksheet.append_row(row_to_add, value_input_option='USER_ENTERED')
            success_message = f"✅ **Success:** Your sale of **${premium_amount:,.2f}** has been recorded successfully!"
            if unrounded_premium != premium_amount:
                success_message += f"\n*(Note: Your input of `{unrounded_premium}` was rounded to two decimal places.)*"
            await interaction.followup.send(success_message, ephemeral=True)

            today_df = get_leaderboard_data('today')
            day_of_week = datetime.datetime.now().strftime('%A')
            today_title = f"📊 Today ({day_of_week}):"
            today_leaderboard = format_leaderboard_section(today_title, today_df)
            await interaction.channel.send(today_leaderboard)
        except Exception as e:
            print(f"Error writing to Google Sheets: {e}")
            await interaction.followup.send("❌ **Error:** Could not write data to database.", ephemeral=True)


# --- SLASH COMMANDS ---
@tree.command(name="sales", description="Press enter to log a new sale.")
async def sales_command(interaction: discord.Interaction):
    await interaction.response.send_modal(SaleEntryModal())


@tree.command(name="leaderboard", description="Display sales leaderboards.")
@app_commands.describe(period="The time period for the leaderboard.")
@app_commands.choices(period=[
    app_commands.Choice(name="Today", value="today"),
    app_commands.Choice(name="Week-to-Date", value="week"),
    app_commands.Choice(name="Month-to-Date", value="month"),
    app_commands.Choice(name="Full Leaderboard", value="full"),
])
async def leaderboard(interaction: discord.Interaction, period: app_commands.Choice[str]):
    await interaction.response.defer(thinking=True, ephemeral=False)

    if period.value == 'full':
        content = get_full_leaderboard_content()
        await interaction.followup.send(content)

    else:
        leaderboard_df = get_leaderboard_data(period.value)
        title = ""
        if period.value == 'today':
            day_of_week = datetime.datetime.now().strftime('%A')
            title = f"📊 Today ({day_of_week}):"
        elif period.value == 'week':
            title = "📅 Week-to-Date:"
        elif period.value == 'month':
            title = "🥇 Month-to-Date:"

        content = format_leaderboard_section(title, leaderboard_df)
        await interaction.followup.send(content)


est_timezone = pytz.timezone('US/Eastern')
post_time = datetime.time(hour=8, minute=0, tzinfo=est_timezone)


@tasks.loop(time=post_time)
async def daily_leaderboard_post():
    """
    This task runs once a day at the specified time and posts the leaderboard.
    """
    await bot.wait_until_ready()

    channel_id = POSTING_CHANNEL_ID
    if not channel_id:
        print("Error: POSTING_CHANNEL_ID is not set. Please check your .env file.")
        return
    
    channel = bot.get_channel(int(channel_id))
    if not channel:
        print(f"Error: Channel with ID {channel_id} not found.")
        return
    
    today_weekday = datetime.datetime.now(est_timezone).weekday()

    print(f"Daily post task running on weekday {today_weekday}")

    if today_weekday == 4:
        print("Happy Friday! Posting full leaderboard.")
        content = get_full_leaderboard_content()
    else:
        print("Posting standard daily leaderboard.")
        content = get_daily_leaderboard_content()

    if content and content.strip():
        await channel.send(content)
    else:
        print("No content to post. Skipping post.")


# --- BOT EVENTS ---
@bot.event
async def on_ready():
    await tree.sync()
    print(f"Logged in as {bot.user} (ID: {bot.user.id}).")
    print("Bot is ready and slash commands are synced.")
    print("------")
    daily_leaderboard_post.start()
    print("Daily leaderboard task started.")
    print("------")


# --- PRIMARY ENTRY POINT ---
if __name__ == "__main__":
    if DISCORD_TOKEN:
        bot.run(DISCORD_TOKEN)
    else:
        print("Error: DISCORD_TOKEN is not set. Please check your .env file.")
        exit()
