import discord
from discord import app_commands
from discord.ext import tasks
from discord.ui import Modal, TextInput, Select, View
import os
import gspread
from dotenv import load_dotenv
import datetime
import pandas as pd
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

# --- ASYNCHRONOUS HELPER FUNCTIONS ---

async def fetch_all_records_async():
    """Asynchronously fetches all records from the main worksheet."""
    return await asyncio.to_thread(worksheet.get_all_records)

async def fetch_teams_from_sheet_async() -> list[str]:
    """Asynchronously fetches the list of teams and updates the cache."""
    global TEAMS_CACHE
    try:
        print("Fetching teams from Google Sheet...")
        team_list_raw = await asyncio.to_thread(teams_worksheet.col_values, 1)
        teams = [team for team in team_list_raw[1:] if team]
        if teams:
            TEAMS_CACHE = teams
            print(f"✅ Teams cache updated with {len(teams)} teams.")
        else:
            print("⚠️ No teams found in sheet.")
        return teams
    except Exception as e:
        print(f"An error occurred while fetching teams: {e}")
        TEAMS_CACHE = [] # Clear cache on error
        return []

def get_teams_from_cache() -> list[str]:
    """Gets the list of teams from the in-memory cache."""
    return TEAMS_CACHE

# --- LEADERBOARD LOGIC (SYNCHRONOUS) ---
# These functions now take data as an argument instead of fetching it themselves.

def process_leaderboard_data(records: list, period: str) -> pd.DataFrame:
    """Processes records into a leaderboard DataFrame. This is CPU-bound and synchronous."""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    required_cols = ['Date', 'User ID', 'Name', 'Premium', 'Team']
    for col in required_cols:
        if col not in df.columns:
            print(f"Error: Sheet is missing required column: '{col}'")
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
    else: # 'full'
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

def format_leaderboard_section(title: str, leaderboard_df: pd.DataFrame) -> str:
    """Formats a leaderboard DataFrame into a string for Discord."""
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


# --- UI COMPONENTS (RE-ARCHITECTED) ---

class SaleEntryModal(Modal, title='Enter Sale Details'):
    # This modal now ONLY contains the text input.
    premium = TextInput(label='Annual Premium Amount', placeholder='e.g., 1250.75', required=True)

    def __init__(self, selected_team: str):
        super().__init__()
        # Store the team selected in the previous step (the view).
        self.selected_team = selected_team

    async def on_submit(self, interaction: discord.Interaction):
        # Defer the response to the modal submission.
        await interaction.response.defer(ephemeral=True, thinking=True)
        
        premium_str = self.premium.value
        # Use the team passed during initialization.
        team_selection = self.selected_team

        try:
            unrounded_premium = float(premium_str.replace(',', ''))
            premium_amount = round(unrounded_premium, 2)
        except ValueError:
            await interaction.followup.send("❌ **Error:** Please enter a valid number for the premium.", ephemeral=True)
            return

        row_to_add = [
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            str(interaction.user.id),
            interaction.user.display_name,
            premium_amount,
            team_selection
        ]

        try:
            # Asynchronously append the new sale.
            await asyncio.to_thread(worksheet.append_row, row_to_add, value_input_option='USER_ENTERED')
            await asyncio.sleep(2) # Give sheets a moment to process.

            # Asynchronously get data for the confirmation message.
            records = await fetch_all_records_async()
            today_df = await asyncio.to_thread(process_leaderboard_data, records, 'today')
            
            day_of_week = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%A')
            leaderboard_content = format_leaderboard_section(f"📊 Today ({day_of_week}):", today_df)
            
            success_message = f"✅ **Success:** Your sale of **${premium_amount:,.2f}** for team **{team_selection}** has been recorded!"
            full_response = f"{success_message}\n\n{leaderboard_content}"
            # Send a new ephemeral message as a followup to the modal submission.
            await interaction.followup.send(full_response, ephemeral=True)
        except Exception as e:
            print(f"CRITICAL ERROR: Error during sale submission: {e}")
            await interaction.followup.send("❌ **Error:** Could not write data to the database. Please try again later.", ephemeral=True)

class TeamSelect(Select):
    """The dropdown menu that will trigger the modal."""
    def __init__(self, teams: list[str]):
        options = [discord.SelectOption(label=team) for team in teams]
        if not options:
            options = [discord.SelectOption(label="No Teams Available", value="NO_TEAMS")]
        super().__init__(placeholder="Select the team for this sale...", options=options, disabled=(not teams))

    async def callback(self, interaction: discord.Interaction):
        # When a team is selected, show the modal.
        selected_team = self.values[0]
        await interaction.response.send_modal(SaleEntryModal(selected_team=selected_team))

        # After the modal is sent, edit the original message to remove the view.
        # This prevents the "Unknown Message" error and provides a clean UX.
        await interaction.edit_original_response(content="Loading sale entry...", view=None)

class TeamSelectView(View):
    """A View to hold the TeamSelect dropdown."""
    def __init__(self, teams: list[str]):
        super().__init__(timeout=180) # The user has 3 minutes to make a selection.
        self.add_item(TeamSelect(teams))

# --- SLASH COMMANDS ---
@tree.command(name="sales", description="Log a new sale by first selecting a team.")
async def sales_command(interaction: discord.Interaction):
    teams = get_teams_from_cache()
    if not teams:
        await interaction.response.send_message("❌ **Error:** The list of teams is currently unavailable. Please try again in a moment.", ephemeral=True)
        # Attempt to refresh the cache in the background.
        asyncio.create_task(fetch_teams_from_sheet_async())
        return
    
    # Create the view and send it ephemerally.
    view = TeamSelectView(teams=teams)
    await interaction.response.send_message("Please select the team for this sale:", view=view, ephemeral=True)

@tree.command(name="leaderboard", description="Display sales leaderboards.")
@app_commands.describe(period="The time period for the leaderboard.")
@app_commands.choices(period=[
    app_commands.Choice(name="Today", value="today"),
    app_commands.Choice(name="Week-to-Date", value="week"),
    app_commands.Choice(name="Month-to-Date", value="month"),
    app_commands.Choice(name="All-Time", value="full"),
])
async def leaderboard(interaction: discord.Interaction, period: app_commands.Choice[str]):
    await interaction.response.defer(thinking=True, ephemeral=False)
    
    records = await fetch_all_records_async()
    leaderboard_df = await asyncio.to_thread(process_leaderboard_data, records, period.value)

    title_map = {
        'today': f"📊 Today ({datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%A')}):",
        'week': "📅 Week-to-Date:",
        'month': "🥇 Month-to-Date:",
        'full': "🏆 All-Time Leaderboard:"
    }
    title = title_map.get(period.value)
    content = format_leaderboard_section(title, leaderboard_df)
    await interaction.followup.send(content)


# --- BACKGROUND TASKS ---
@tasks.loop(minutes=10)
async def update_teams_cache_loop():
    await fetch_teams_from_sheet_async()

@tasks.loop(time=datetime.time(hour=8, minute=0, tzinfo=pytz.timezone('US/Eastern')))
async def daily_leaderboard_post():
    channel_id_str = POSTING_CHANNEL_ID
    if not channel_id_str:
        print("Error: POSTING_CHANNEL_ID is not set.")
        return
    
    channel = bot.get_channel(int(channel_id_str))
    if not channel:
        print(f"Error: Channel with ID {channel_id_str} not found.")
        return

    print("Executing daily leaderboard post...")
    records = await fetch_all_records_async()
    
    # Process all dataframes in parallel
    today_df, week_df, month_df = await asyncio.gather(
        asyncio.to_thread(process_leaderboard_data, records, 'today'),
        asyncio.to_thread(process_leaderboard_data, records, 'week'),
        asyncio.to_thread(process_leaderboard_data, records, 'month')
    )

    est_timezone = pytz.timezone('US/Eastern')
    now = datetime.datetime.now(est_timezone)
    today_title = f"📊 Today ({now.strftime('%A')}):"
    
    today_content = format_leaderboard_section(today_title, today_df)
    week_content = format_leaderboard_section("📅 Week-to-Date:", week_df)
    month_content = format_leaderboard_section("🥇 Month-to-Date:", month_df)
    
    # Friday full post
    if now.weekday() == 4:
        # Note: Rising Star and Top Performer logic could also be made async for full optimization
        content = f"{today_content}\n\n{week_content}\n\n{month_content}" # Simplified for now
    else:
        content = f"{today_content}\n\n{week_content}\n\n{month_content}"
        
    await channel.send(content)

# --- BOT EVENTS ---
@bot.event
async def on_ready():
    await tree.sync()
    print(f"Logged in as {bot.user} (ID: {bot.user.id}).")
    print("Bot is ready and slash commands are synced.")
    print("------")
    
    # Perform initial cache population before starting loops
    await fetch_teams_from_sheet_async()
    
    # Start all background tasks
    update_teams_cache_loop.start()
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