import logging

# Configure the logger
logging.basicConfig(
    filename="logger.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# import logging
# import os
# import html
# import json
# from services.config import redis_client  # Import your Redis client
#
#
# class HTMLFormatter(logging.Formatter):
#     COLOR_MAP = {
#         logging.INFO: "#2ecc71",
#         logging.WARNING: "#f39c12",
#         logging.ERROR: "#e74c3c",
#         logging.DEBUG: "#3498db",
#         logging.CRITICAL: "#c0392b",
#     }
#
#     def format(self, record):
#         color = self.COLOR_MAP.get(record.levelno, "#2c3e50")
#         levelname = f'<span class="log-level" style="color: {color};">{record.levelname}</span>'
#         message = html.escape(record.msg)  # Escape the message to prevent HTML issues
#         return f'''
#         <div class="log-entry" data-level="{record.levelname}">
#             <div class="log-meta">
#                 <span class="log-time">{self.formatTime(record)}</span>
#                 {levelname}
#             </div>
#             <div class="log-message">{message}</div>
#         </div>'''
#
#
# class RedisBackedHTMLLogger(logging.Handler):
#     REDIS_LOG_KEY = "logs:logger_data"  # Key to store logs in Redis
#
#     def __init__(self, filename):
#         super().__init__()
#         self.filename = filename
#         if not os.path.exists(filename):
#             self.initialize_file()
#         self.populate_logs_from_redis()
#
#     def initialize_file(self):
#         with open(self.filename, 'w', encoding='utf-8') as f:
#             f.write(f'''<!DOCTYPE html>
# <html lang="en">
# <head>
#     <meta charset="UTF-8">
#     <meta name="viewport" content="width=device-width, initial-scale=1.0">
#     <title>Application Logs</title>
#     <style>
#         /* Include the CSS styles here */
#     </style>
# </head>
# <body>
#     <header>
#         <div class="app-title">Application Logs</div>
#     </header>
#     <div class="log-container">
#     </div>
#     <footer>
#         Logging System • {os.environ.get('USER', 'Admin')} • Generated at startup
#     </footer>
# </body>
# </html>''')
#
#     def populate_logs_from_redis(self):
#         try:
#             # Fetch all logs from Redis
#             logs = redis_client.lrange(self.REDIS_LOG_KEY, 0, -1)  # Assuming logs are stored in a list
#             log_entries = [json.loads(log) for log in logs]  # Deserialize logs
#             formatted_logs = [self.format(logging.makeLogRecord(log)) for log in log_entries]
#
#             # Insert into log container
#             with open(self.filename, 'r', encoding='utf-8') as f:
#                 content = f.read()
#
#             start_marker = '<div class="log-container">'
#             start_index = content.find(start_marker) + len(start_marker)
#             if start_index == -1:
#                 raise ValueError("Start marker not found in the log file.")
#
#             updated_content = content[:start_index] + "\n".join(formatted_logs) + content[start_index:]
#             with open(self.filename, 'w', encoding='utf-8') as f:
#                 f.write(updated_content)
#         except Exception as e:
#             logging.error("Failed to populate logs from Redis: %s", e)
#
#     def emit(self, record):
#         try:
#             # Format the log message
#             msg = self.format(record)
#
#             # Push the log into Redis
#             log_entry = record.__dict__
#             redis_client.rpush(self.REDIS_LOG_KEY, json.dumps(log_entry))  # Store log in Redis as JSON
#
#             # Read and update the log HTML file
#             with open(self.filename, 'r', encoding='utf-8') as f:
#                 content = f.read()
#
#             start_marker = '<div class="log-container">'
#             start_index = content.find(start_marker) + len(start_marker)
#             if start_index == -1:
#                 raise ValueError("Start marker not found in the log file.")
#
#             updated_content = content[:start_index] + f'\n{msg}' + content[start_index:]
#             with open(self.filename, 'w', encoding='utf-8') as f:
#                 f.write(updated_content)
#         except Exception as e:
#             logging.error("Failed to write log entry: %s", e)
#             self.handleError(record)
#
#
# # Configure the logger
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# logger.handlers.clear()
#
# html_handler = RedisBackedHTMLLogger("logs.html")
# html_handler.setFormatter(HTMLFormatter("%(asctime)s"))
# logger.addHandler(html_handler)
#
# # Example Usage
# logger.info("Server started successfully.")
# logger.warning("This is a warning message.")
# logger.error("An error occurred.")
