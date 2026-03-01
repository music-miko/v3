# Copyright (c) 2025 AnonymousX1025
# Licensed under the MIT License.
# This file is part of AnonXMusic


import time
import asyncio
import pytz
from datetime import datetime, timedelta

from pyrogram import enums, errors, filters, types

from anony import anon, app, config, db, lang, logger, queue, tasks, userbot, yt
from anony.helpers import buttons


@app.on_message(filters.video_chat_started, group=19)
@app.on_message(filters.video_chat_ended, group=20)
async def _watcher_vc(_, m: types.Message):
    await anon.stop(m.chat.id)


async def auto_leave():
    # Use pytz just like your old working script for reliable IST
    IST = pytz.timezone("Asia/Kolkata")
    
    while True:
        now_ist = datetime.now(IST)
        target = now_ist.replace(hour=5, minute=30, second=0, microsecond=0)
        
        # If 4:45 AM IST has already passed today, schedule for tomorrow
        if now_ist >= target:
            target += timedelta(days=1)
            
        wait_seconds = (target - now_ist).total_seconds()
        
        # Log the sleep schedule to the terminal so you know it's working
        logger.info(f"Next Auto-Leave scheduled for: {target.strftime('%Y-%m-%d %H:%M:%S %Z')} (Sleeping for {int(wait_seconds)}s)")
        await asyncio.sleep(wait_seconds)
        
        logger.info("Starting Auto-Leave cleanup cycle...")
        
        for ub in userbot.clients:
            chats = []
            try:
                # Safely gather all dialogs, handling potential floodwaits
                async for dialog in ub.get_dialogs():
                    if dialog.chat.type in [
                        enums.ChatType.GROUP, enums.ChatType.SUPERGROUP,
                    ]:
                        chats.append(dialog.chat.id)
            except errors.FloodWait as e:
                logger.warning(f"FloodWait encountered while fetching dialogs: {e.value}s")
                await asyncio.sleep(e.value + 2)
            except Exception as e:
                logger.error(f"Error fetching dialogs: {e}")
                pass
                
            for chat in chats:
                if chat in [app.logger, -1001686672798, -1001549206010]:
                    continue
                if chat in db.active_calls:
                    continue
                
                # FloodWait Retry mechanism
                retries = 3
                while retries > 0:
                    try:
                        await ub.leave_chat(chat)
                        logger.info(f"Userbot successfully left chat ID: {chat}")
                        await asyncio.sleep(5)  # Safe delay between leaves
                        break  # Break out of the retry loop on success
                        
                    except errors.FloodWait as e:
                        # If Telegram says wait, sleep for the penalty time + 2s buffer
                        logger.warning(f"FloodWait of {e.value}s while leaving {chat}. Retrying...")
                        await asyncio.sleep(e.value + 2)
                        retries -= 1
                        
                    except Exception as e:
                        # Break out of loop for other errors (e.g. already left, banned, etc.)
                        break
                        
        logger.info("Auto-Leave cleanup cycle completed.")


async def track_time():
    while True:
        await asyncio.sleep(1)
        for chat_id in list(db.active_calls):
            if not await db.playing(chat_id):
                continue
            media = queue.get_current(chat_id)
            if not media:
                continue
            media.time += 1


async def update_timer(length=10):
    while True:
        await asyncio.sleep(7)
        for chat_id in list(db.active_calls):
            if not await db.playing(chat_id):
                continue
            try:
                media = queue.get_current(chat_id)
                duration, message_id = media.duration_sec, media.message_id
                if not duration or not message_id or not media.time:
                    continue
                played = media.time
                remaining = duration - played
                pos = min(int((played / duration) * length), length - 1)
                timer = "—" * pos + "◉" + "—" * (length - pos - 1)

                if remaining <= 30:
                    # Renamed 'next' to 'next_track' and passed it directly to the async task
                    next_track = queue.get_next(chat_id, check=True)
                    if next_track and not next_track.file_path and not getattr(next_track, "is_downloading", False):
                        next_track.is_downloading = True
                        
                        # Pass the track explicitly so it doesn't get overwritten by the loop
                        async def fetch_next(track):
                            try:
                                track.file_path = await yt.download(track.id, video=track.video)
                            except Exception:
                                pass
                            finally:
                                track.is_downloading = False
                                
                        asyncio.create_task(fetch_next(next_track))

                if remaining < 10:
                    remove = True
                else:
                    if config.THUMB_GEN:
                        timer = f"{time.strftime('%M:%S', time.gmtime(played))} | {timer} | -{time.strftime('%M:%S', time.gmtime(remaining))}"
                    else:
                        timer = None
                    remove = False

                if not timer and not remove:
                    continue

                await app.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=buttons.controls(
                        chat_id=chat_id, timer=timer, remove=remove
                    ),
                )
            except Exception:
                pass


async def vc_watcher(sleep=15):
    while True:
        await asyncio.sleep(sleep)
        for chat_id in list(db.active_calls):
            client = await db.get_assistant(chat_id)
            media = queue.get_current(chat_id)
            participants = await client.get_participants(chat_id)
            if len(participants) < 2 and media.time > 120:
                _lang = await lang.get_lang(chat_id)
                try:
                    sent = await app.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=media.message_id,
                        reply_markup=buttons.controls(
                            chat_id=chat_id, status=_lang["stopped"], remove=True
                        ),
                    )
                    await anon.stop(chat_id)
                    await sent.reply_text(_lang["auto_left"])
                except errors.MessageIdInvalid:
                    pass


if config.AUTO_END:
    tasks.append(asyncio.create_task(vc_watcher()))

# Made this true always as requested
tasks.append(asyncio.create_task(auto_leave()))

tasks.append(asyncio.create_task(track_time()))
tasks.append(asyncio.create_task(update_timer()))
