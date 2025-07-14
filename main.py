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
TEAMS_AND_ROLES_CACHE = {}


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

async def fetch_teams_and_roles_from_sheet_async() -> list[str]:
    """Asynchronously fetches the list of teams and role IDs and updates the cache."""
    global TEAMS_AND_ROLES_CACHE
    try:
        print("Fetching teams from Google Sheet...")
        all_values = await asyncio.to_thread(teams_worksheet.get_all_values)
        new_cache = {
            row[0]: {"role": row[1], "channel": row[2]}
            for row in all_values[1:] if row and len(row) > 2 and row[0] and row[1] and row[2]
        }
        if new_cache:
            TEAMS_AND_ROLES_CACHE = new_cache
            print(f"✅ Teams cache updated with {len(TEAMS_AND_ROLES_CACHE)} teams.")
        else:
            print("⚠️ No teams found in sheet.")
            TEAMS_AND_ROLES_CACHE = {}
    except Exception as e:
        print(f"An error occurred while fetching teams: {e}")
        TEAMS_AND_ROLES_CACHE = {}
        return []

def get_teams_from_cache() -> list[str]:
    """Gets the list of teams from the in-memory cache."""
    return TEAMS_AND_ROLES_CACHE.keys()


# --- LEADERBOARD LOGIC (SYNCHRONOUS) ---
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
    else:
        df_filtered = df
    
    if df_filtered.empty:
        return pd.DataFrame()
        
    leaderboard = df_filtered.groupby(['User ID', 'Name']).agg(
        TotalPremium=('Premium', 'sum'),
        SaleCount=('Premium', 'count'),
    ).reset_index()

    leaderboard_sorted = leaderboard.sort_values('TotalPremium', ascending=False).head(20)
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


def process_team_leaderboard_data(records: list, period: str) -> pd.DataFrame:
    """Processes records into a team leaderboard DataFrame."""
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)

    required_cols = ['Date', 'Premium', 'Team']
    for col in required_cols:
        if col not in df.columns:
            print(f"Error: sheet is missing required column: {col}")

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Premium'] = pd.to_numeric(df['Premium'], errors='coerce')
    df.dropna(subset=['Date', 'Premium', 'Team'], inplace=True)
    df = df[df['Team'] != '']

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
    
    team_leaderboard = df_filtered.groupby('Team').agg(
        TotalPremium=('Premium', 'sum'),
        SaleCount=('Premium', 'count'),
    ).reset_index()

    team_leaderboard_sorted = team_leaderboard.sort_values('TotalPremium', ascending=False)
    team_leaderboard_sorted.reset_index(drop=True, inplace=True)
    team_leaderboard_sorted.index += 1
    return team_leaderboard_sorted


def format_team_leaderboard_section(title: str, leaderboard_df: pd.DataFrame) -> str:
    """Formats a team leaderboard DataFrame into a string for Discord."""
    if leaderboard_df.empty:
        return f"**{title}**\n*No entries yet for this period.*"
    
    lines = [f"**{title}**"]
    for rank, row in leaderboard_df.iterrows():
        team_name = row['Team']
        premium_formatted = f"${row['TotalPremium']:,.2f}"
        sale_count = int(row['SaleCount'])

        team_data = TEAMS_AND_ROLES_CACHE.get(team_name, {})
        role_id = team_data.get('role')

        team_mention = f"<@&{role_id}>" if role_id else team_name

        lines.append(f"{rank}. {team_mention}: {premium_formatted} | {sale_count} FP")
    return "\n".join(lines)


# --- UI COMPONENTS (RE-ARCHITECTED) ---
class SaleEntryModal(Modal, title='Enter Sale Details'):
    premium = TextInput(label='Annual Premium Amount', placeholder='e.g., 1250.75', required=True)

    def __init__(self, selected_team: str):
        super().__init__()
        self.selected_team = selected_team

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        
        premium_str = self.premium.value
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
            await asyncio.to_thread(worksheet.append_row, row_to_add, value_input_option='USER_ENTERED')
            
            success_message = f"✅ **Success:** Your sale of **{premium_amount:,.2f}** has been recorded!.\n"
            await interaction.followup.send(success_message, ephemeral=True)

            posting_channel_id_str = POSTING_CHANNEL_ID
            if not posting_channel_id_str:
                print("Error: POSTING_CHANNEL_ID is not set.")
                return
            
            try:
                posting_channel = bot.get_channel(int(posting_channel_id_str))
                if posting_channel:
                    embed = discord.Embed(
                        title="💰 New Sale!",
                        description=f"{interaction.user.mention} just made a sale!",
                        color=discord.Color.blue()
                    )
                    embed.add_field(name="Sale Amount", value=f"${premium_amount:,.2f}", inline=False)
                    embed.set_thumbnail(url=interaction.user.display_avatar.url)
                    embed.set_footer(text=f"Team: {team_selection}")
                    embed.timestamp = datetime.datetime.now(pytz.utc)

                    await posting_channel.send(embed=embed)
                else:
                    print(f"Error: Channel with ID {posting_channel_id_str} not found.")
            except Exception as e:
                print(f"An error occurred while sending the sale notification: {e}")

        except Exception as e:
            print(f"CRITICAL ERROR: Error during sale entry: {e}")
            await interaction.followup.send("❌ **Error:** An unexpected error occurred while recording your sale. Please try again later.", ephemeral=True)
            return


class TeamSelect(Select):
    """The dropdown menu that will trigger the modal."""
    def __init__(self, teams: list[str]):
        options = [discord.SelectOption(label=team) for team in teams]
        if not options:
            options = [discord.SelectOption(label="No Teams Available", value="NO_TEAMS")]
        super().__init__(placeholder="Select the team for this sale...", options=options, disabled=(not teams))

    async def callback(self, interaction: discord.Interaction):
        selected_team = self.values[0]
        await interaction.response.send_modal(SaleEntryModal(selected_team=selected_team))

        await interaction.edit_original_response(content="Loading sale entry...", view=None)


class TeamSelectView(View):
    """A View to hold the TeamSelect dropdown."""
    def __init__(self, teams: list[str]):
        super().__init__(timeout=180)
        self.add_item(TeamSelect(teams))


# --- SLASH COMMANDS ---
@tree.command(name="sales", description="Log a new sale by first selecting a team.")
async def sales_command(interaction: discord.Interaction):
    teams = get_teams_from_cache()
    if not teams:
        await interaction.response.send_message("❌ **Error:** The list of teams is currently unavailable. Please try again in a moment.", ephemeral=True)
        asyncio.create_task(fetch_teams_and_roles_from_sheet_async())
        return
    
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


@tree.command(name="teams", description="Display team sales leaderboards.")
async def teams_leaderboard(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=False)

    records = await fetch_all_records_async()

    today_df, week_df, month_df = await asyncio.gather(
        asyncio.to_thread(process_team_leaderboard_data, records, 'today'),
        asyncio.to_thread(process_team_leaderboard_data, records, 'week'),
        asyncio.to_thread(process_team_leaderboard_data, records, 'month')
    )

    est_timezone = pytz.timezone('US/Eastern')
    now = datetime.datetime.now(est_timezone)
    today_title = f"📊 Today ({now.strftime('%A')}):"

    today_content = format_team_leaderboard_section(today_title, today_df)
    week_content = format_team_leaderboard_section("📅 Week-to-Date:", week_df)
    month_content = format_team_leaderboard_section("🥇 Month-to-Date:", month_df)

    content = f"**🏆 Team Leaderboards 🏆**\n\n{today_content}\n\n{week_content}\n\n{month_content}"

    await interaction.followup.send(content)


# @tree.command(name="dev", description="Developer command to test bot functionality.")
# async def dev_command(interaction: discord.Interaction):
#     await interaction.response.send_message("Manually triggered developer command. This is a placeholder for testing purposes.", ephemeral=True)
#     asyncio.create_task(run_daily_team_leaderboards_post())


# --- BACKGROUND TASKS ---
@tasks.loop(minutes=10)
async def update_teams_cache_loop():
    await fetch_teams_and_roles_from_sheet_async()

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
    
    if now.weekday() == 4:
        content = f"{today_content}\n\n{week_content}\n\n{month_content}"
    else:
        content = f"{today_content}\n\n{week_content}\n\n{month_content}"
        
    await channel.send(content)


async def run_daily_team_leaderboards_post():
    print("Executing daily team-specific leaderboard post...")
    all_records = await fetch_all_records_async()
    if not all_records:
        print("No records found for daily team leaderboard post.")
        return
    
    all_records_df = pd.DataFrame(all_records)

    for team_name, team_data in TEAMS_AND_ROLES_CACHE.items():
        channel_id = team_data.get('channel')
        if not channel_id:
            print(f"Warning: No channel found for team '{team_name}'. Skipping post.")
            continue

        channel = bot.get_channel(int(channel_id))
        if not channel:
            print(f"Error: Channel with ID {channel_id} not found for team '{team_name}'. Skipping post.")
            continue

        team_records_df = all_records_df[all_records_df['Team'] == team_name]
        if team_records_df.empty:
            print(f"No records found for team '{team_name}'.")
            await channel.send(f"**🏆 Daily Leaderboard for {team_name} 🏆**\n\nNo sales recorded yet for today, this week, or this month.")
            continue

        team_records = team_records_df.to_dict('records')

        today_df, week_df, month_df = await asyncio.gather(
            asyncio.to_thread(process_leaderboard_data, team_records, 'today'),
            asyncio.to_thread(process_leaderboard_data, team_records, 'week'),
            asyncio.to_thread(process_leaderboard_data, team_records, 'month')
        )

        est_timezone = pytz.timezone('US/Eastern')
        now = datetime.datetime.now(est_timezone)
        today_title = f"📊 Today ({now.strftime('%A')}):"

        today_content = format_leaderboard_section(today_title, today_df)
        week_content = format_leaderboard_section("📅 Week-to-Date:", week_df)
        month_content = format_leaderboard_section("🥇 Month-to-Date:", month_df)

        content = f"**🏆 Daily Leaderboard for {team_name} 🏆**\n\n{today_content}\n\n{week_content}\n\n{month_content}"

        try:
            await channel.send(content)
            print(f"Successfully posted daily leaderboard for team '{team_name}' in channel {channel.name}.")
        except Exception as e:
            print(f"Error posting daily leaderboard for team '{team_name}' in channel {channel.name}: {e}")

        await asyncio.sleep(1)


@tasks.loop(time=datetime.time(hour=12, minute=0, tzinfo=pytz.timezone('US/Eastern')))
async def daily_team_leaderboards_post():
    print("Executing daily team-specific leaderboard post...")
    await run_daily_team_leaderboards_post()


# --- BOT EVENTS ---
@bot.event
async def on_ready():
    await tree.sync()
    print(f"Logged in as {bot.user} (ID: {bot.user.id}).")
    print("Bot is ready and slash commands are synced.")
    print("------")
    
    await fetch_teams_and_roles_from_sheet_async()
    
    update_teams_cache_loop.start()
    daily_leaderboard_post.start()
    daily_team_leaderboards_post.start()
    
    print("All background tasks started.")
    print("------")


# --- PRIMARY ENTRY POINT ---
if __name__ == "__main__":
    if DISCORD_TOKEN:
        bot.run(DISCORD_TOKEN)
    else:
        print("Error: DISCORD_TOKEN is not set. Please check your .env file.")
        exit()