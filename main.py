import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import Modal, TextInput, Select
import os
import gspread
from dotenv import load_dotenv
import datetime
import pandas as pd
import calendar
import asyncio

import pytz

# --- CONFIGURATION ---
load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
POSTING_CHANNEL_ID = os.getenv('POSTING_CHANNEL_ID')
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
GOOGLE_SPREADSHEET_NAME = os.getenv('GOOGLE_SPREADSHEET_NAME')
GOOGLE_WORKSHEET_NAME = os.getenv('GOOGLE_WORKSHEET_NAME')
GOOGLE_TEAMS_WORKSHEET_NAME = os.getenv('GOOGLE_TEAMS_WORKSHEET_NAME')


# --- CACHE ---
TEAMS_CACHE = []


# --- Google Sheets Setup ---
try:    
    gc = gspread.service_account(filename=GOOGLE_SERVICE_ACCOUNT_FILE)
    sh = gc.open(GOOGLE_SPREADSHEET_NAME)
    worksheet = sh.worksheet(GOOGLE_WORKSHEET_NAME)
    teams_worksheet = sh.worksheet(GOOGLE_TEAMS_WORKSHEET_NAME)
    print("Google Sheets connected successfully.")
except gspread.exceptions.SpreadsheetNotFound:
    print("Error: Google Sheets spreadsheet not found. Please check the name and make sure that the spreadsheet is shared with the bot's service account email.")
    exit()
except gspread.exceptions.WorksheetNotFound:
    print(f"Error: A required worksheet was not found. Please ensure both '{GOOGLE_WORKSHEET_NAME}' and '{GOOGLE_TEAMS_WORKSHEET_NAME}' worksheets exist.")
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
def _fetch_teams_from_sheet() -> list[str]:
    """Synchronously fetches the list of teams from the Google Sheet."""
    try:
        teams_ws = sh.worksheet(GOOGLE_TEAMS_WORKSHEET_NAME)
        team_list = teams_ws.col_values(1)
        return [team for team in team_list[1:] if team]
    except Exception as e:
        print(f"An error occurred while fetching teams: {e}")
        return []


def get_teams() -> list[str]:
    """
    Fetches a list of teams from the in-memory cache.
    """
    return TEAMS_CACHE


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

        required_cols = ['Date', 'User ID', 'Name', 'Premium', 'Team']
        if not all(col in df.columns for col in required_cols):
            print(f"Error: Sheet is missing one of the required columns: {required_cols}")
            if 'Team' not in df.columns:
                df['Team'] = 'N/A'
            if not all(col in df.columns for col in required_cols):
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

        sales_this_week = df_this_week.groupby('User ID')['Premium'].sum()
        sales_last_week = df_last_week.groupby('User ID')['Premium'].sum()

        if sales_last_week.empty:
            return ""

        merged = pd.merge(sales_this_week.to_frame(), sales_last_week.to_frame(), on='User ID', how='inner', suffixes=('_this', '_last'))
        
        if merged.empty:
            return ""

        merged['Growth'] = ((merged['Premium_this'] - merged['Premium_last']) / merged['Premium_last']) * 100
        
        rising_stars = merged.sort_values(by='Growth', ascending=False)
        
        if rising_stars.empty or rising_stars.iloc[0]['Growth'] <= 0:
            return ""

        top_riser_id = rising_stars.index[0]

        user_info = df[df['User ID'] == top_riser_id].iloc[0]
        
        user_mention = f"<@{str(user_info['User ID']).split('.')[0]}>"
        this_week_total = sales_this_week[top_riser_id]
        last_week_total = sales_last_week[top_riser_id]
        growth_percent = rising_stars.iloc[0]['Growth']
        
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

    est_timezone = pytz.timezone('US/Eastern')
    day_of_week = datetime.datetime.now(est_timezone).strftime('%A')
    today_title = f"📊 Today ({day_of_week}):"
    today_content = format_leaderboard_section(today_title, today_df)

    week_title = "📅 Week-to-Date:"
    week_content = format_leaderboard_section(week_title, week_df)

    month_title = "🥇 Month-to-Date:"
    month_content = format_leaderboard_section(month_title, month_df)

    content_sections = [
        c for c in [
            top_performer_content,
            today_content,
            week_content,
            month_content,
            rising_star_content,
        ] if c
    ]
    return "\n\n".join(content_sections)


def get_daily_leaderboard_content() -> str:
    """Generates the string content for the standard daily leaderboard."""
    today_df = get_leaderboard_data('today')
    week_df = get_leaderboard_data('week')
    month_df = get_leaderboard_data('month')

    est_timezone = pytz.timezone('US/Eastern')
    day_of_week = datetime.datetime.now(est_timezone).strftime('%A')
    today_title = f"📊 Today ({day_of_week}):"
    today_content = format_leaderboard_section(today_title, today_df)

    week_title = "📅 Week-to-Date:"
    week_content = format_leaderboard_section(week_title, week_df)

    month_title = "🥇 Month-to-Date:"
    month_content = format_leaderboard_section(month_title, month_df)

    return f"{today_content}\n\n{week_content}\n\n{month_content}"


# --- MODAL DEFINITION ---
class TeamSelect(Select):
    def __init__(self, teams: list[str]):
        options = [discord.SelectOption(label=team) for team in teams]
        if not options:
            options = [discord.SelectOption(label="No Teams Available", description="Contact an admin to set up teams.", value="NO_TEAMS")]

        super().__init__(
            placeholder="Select your team...",
            min_values=1,
            max_values=1,
            options=options,
            disabled=(not teams)
        )

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()


class SaleEntryModal(Modal, title='Enter Sale Details'):
    premium = TextInput(label='Annual Premium Amount', placeholder='e.g., 1250.75', required=True)

    def __init__(self,teams: list[str]):
        super().__init__()
        self.team_select = TeamSelect(teams)
        self.add_item(self.team_select)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        submission_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        user_id = interaction.user.id
        discord_username = interaction.user.display_name
        premium_amount_str = self.premium.value

        team_selection = self.team_select.values[0]
        
        try:
            unrounded_premium = float(premium_amount_str.replace(',', ''))
            premium_amount = round(unrounded_premium, 2)
        except ValueError:
            await interaction.followup.send("❌ **Error:** Please enter a valid number.", ephemeral=True)
            return

        try:
            row_to_add = [submission_date, str(user_id), discord_username, premium_amount, team_selection]
            # Run the synchronous gspread call in an executor to avoid blocking
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: worksheet.append_row(row_to_add, value_input_option='USER_ENTERED'))
            
            await asyncio.sleep(2) # Give sheets a moment to update

            leaderboard_content = ""
            try:
                today_df = await loop.run_in_executor(None, get_leaderboard_data, 'today')
                est_timezone = pytz.timezone('US/Eastern')
                day_of_week = datetime.datetime.now(est_timezone).strftime('%A')
                today_title = f"📊 Today ({day_of_week}):"
                leaderboard_content = format_leaderboard_section(today_title, today_df)
            except Exception as e:
                print(f"NON-CRITICAL: Leaderboard generation failed, will proceed without it. Error: {e}")
                leaderboard_content = "\n\n*(Could not retrieve the updated leaderboard at this time.)*"

            success_message = f"✅ **Success:** Your sale of **${premium_amount:,.2f}** for team **{team_selection}** has been recorded successfully!"
            if unrounded_premium != premium_amount:
                success_message += f"\n*(Note: Your input of `{unrounded_premium}` was rounded to two decimal places.)*"

            full_response = f"{success_message}\n\n{leaderboard_content}"
            await interaction.followup.send(full_response, ephemeral=True)

        except Exception as e:
            print(f"CRITICAL ERROR: Error writing to Google Sheets: {e}")
            await interaction.followup.send("❌ **Error:** Could not write data to database. Please try again.", ephemeral=True)


# --- SLASH COMMANDS ---
@tree.command(name="sales", description="Press enter to log a new sale.")
async def sales_command(interaction: discord.Interaction):
    teams = get_teams()
    if not teams: 
        await interaction.response.send_message("❌ **Error:** The list of teams is currently unavailable. Please have an admin check the bot's configuration or try again in a moment.", ephemeral=True)
        return
    
    await interaction.response.send_modal(SaleEntryModal(teams=teams))


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

    loop = asyncio.get_running_loop()
    if period.value == 'full':
        content = await loop.run_in_executor(None, get_full_leaderboard_content)
    else:
        leaderboard_df = await loop.run_in_executor(None, get_leaderboard_data, period.value)
        title_map = {
            'today': f"📊 Today ({datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%A')}):",
            'week': "📅 Week-to-Date:",
            'month': "🥇 Month-to-Date:"
        }
        title = title_map.get(period.value, "Leaderboard")
        content = format_leaderboard_section(title, leaderboard_df)
    
    await interaction.followup.send(content)


# ---Background Tasks---
est_timezone = pytz.timezone('US/Eastern')
post_time = datetime.time(hour=8, minute=0, tzinfo=est_timezone)


@tasks.loop(time=post_time)
async def daily_leaderboard_post():
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
    
    loop = asyncio.get_running_loop()
    if today_weekday == 4:
        print("Happy Friday! Posting full leaderboard.")
        content = await loop.run_in_executor(None, get_full_leaderboard_content)
    else:
        print("Posting standard daily leaderboard.")
        content = await loop.run_in_executor(None, get_daily_leaderboard_content)

    if content and content.strip():
        await channel.send(content)
    else:
        print("No content to post. Skipping post.")

@daily_leaderboard_post.before_loop
async def before_daily_post():
    await bot.wait_until_ready()

@tasks.loop(minutes=10)
async def update_teams_cache():
    """
    Periodically updates the teams cache from the Google Sheet.
    """
    global TEAMS_CACHE
    print("Updating teams cache...")
    loop = bot.loop
    teams = await loop.run_in_executor(None, _fetch_teams_from_sheet)
    if teams:
        TEAMS_CACHE = teams
        print(f"✅ Teams cache updated with {len(teams)} teams.")
    else:
        print("⚠️ Could not update teams cache. No teams found or an error occurred.")

@update_teams_cache.before_loop
async def before_cache_update():
    """Wait until the bot is logged in and ready before starting the cache loop."""
    await bot.wait_until_ready()


# --- BOT EVENTS ---
@bot.event
async def on_ready():
    await tree.sync()
    print(f"Logged in as {bot.user} (ID: {bot.user.id}).")
    print("Bot is ready and slash commands are synced.")
    print("------")
    
    # Start all background tasks. They will run once when ready.
    update_teams_cache.start()
    daily_leaderboard_post.start()
    
    print("All background tasks started.")
    print("------")


# --- PRIMARY ENTRY POINT ---
if __name__ == "__main__":
    if DISCORD_TOKEN:
        bot.run(DISCORD_TOKEN)
    else:
        print("Error: DISCORD_TOKEN is not set. Please check your .env file.")
        exit()