import sqlite3
import HTMLParser
from timeit import default_timer as timer


def parse_messages(filepath):
    message_htm = open(filepath)
    messages = message_htm.read()
    messages = messages.decode('UTF-8')
    message_htm.close()

    insert_smts = []
    relations = {}

    header_partition = messages.partition('<div class="contents">')
    footer_partition = header_partition[2].partition('<div class="footer">')
    body = footer_partition[0]
    body = body.partition('</h1>')
    conversations = remove_tags(body[2])
    conversations = conversations.split('<div class="thread">')

    relation_id = 0

    for conversation in conversations:
        messages = conversation.split('<div class="message">')
        message_id = 0
        for message in messages:
            message_partition = message.partition('<span class="user">')
            message_partition = message_partition[2].partition('<p>')
            text = message_partition[2]
            meta = message_partition[0]
            meta_partition = meta.partition('<span class="meta">')
            username = meta_partition[0]
            date_time = meta_partition[2]
            date_time = date_time.split()

            if '@' not in username and len(date_time) == 7:
                timestamp = get_timestamp(date_time)

                if username not in relations:
                    relations[username] = relation_id
                    relation_id += 1

                insert_smt = (relations[username], message_id, username, timestamp, text)
                insert_smts.append(insert_smt)
                message_id += 1

    if len(insert_smts) > 0:
        add_to_db(insert_smts)


def remove_tags(body):
    body = HTMLParser.HTMLParser().unescape(body)
    body = body.replace('<div>', '')
    body = body.replace('</div>', '')
    body = body.replace('</span>', '')
    body = body.replace('</p>', '')
    return body


def get_timestamp(date_time):
    year = date_time[3]
    month = get_month_num(date_time[1])
    day = get_day_num(date_time[2])
    time = get_military_time(date_time[5])

    # "YYYY-MM-DD HH:MM:SS.SSS"
    timestamp = "%(year)s-%(month)s-%(day)s %(time)s" % locals()

    return timestamp


# input example: Monday, January 11, 2016 at 1:00pm PDT
# output example: 2016-01-11 13:00:00.000
def get_military_time(timestamp):
    # am or pm
    meridiem = timestamp[-2:]
    time = timestamp[:-2].split(':')
    hour = time[0]
    minute = time[1]

    if meridiem == 'pm':
        hour = int(hour)
        hour += 12
        hour = str(hour)

    return "%(hour)s:%(minute)s:00.000" % locals()


def get_day_num(day):
    day = day.strip(',')

    if len(day) == 1:
        day = '0' + day

    return day


def get_month_num(month):
    return {
        'January': '01',
        'February': '02',
        'March': '03',
        'April': '04',
        'May': '05',
        'June': '06',
        'July': '07',
        'August': '08',
        'September': '09',
        'October': '10',
        'November': '11',
        'December': '12'
    }[month]


def create_db():
    c.execute('''
      DROP TABLE IF EXISTS messages
    ''')

    c.execute('''
      CREATE TABLE messages
      (relation_id int, message_id int, username text, message_date text, message text)
    ''')

    conn.commit()


def add_to_db(insert_smts):
    c.executemany('INSERT INTO messages VALUES (?,?,?,?,?)', insert_smts)
    conn.commit()


def get_row_count():
    c.execute('SELECT COUNT(*) FROM messages')
    conn.commit()
    print c.fetchone()


conn = sqlite3.connect('fb_messages.db')
c = conn.cursor()
create_db()
start = timer()
parse_messages('data/messages.htm')
end = timer()
print(end - start)
get_row_count()
conn.close()
