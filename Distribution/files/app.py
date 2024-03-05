from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import work
import frame
import matplotlib.pyplot as plt
import numpy as np
import io
import base64

app = Flask(__name__)
d = frame.Data()


@app.route('/')
def index():
    data_count = d.count_of_data()
    return render_template('index.html', data_count=data_count)


@app.route('/parse', methods=['POST'])
def parse_handler():
    data = request.form
    start_interval = int(data['start_interval'])
    end_interval = int(data['end_interval'])
    arg = get_date(start_interval, end_interval)
    pr = work.Worker(*arg)
    pr.run()

    d.update_count_of_data()
    return 'Parsing finished. Data count: {}'.format(d.data_count)


@app.route('/get_data_count', methods=['GET'])
def get_data_count():
    return str(d.count_of_data())


def get_date(left, timedelta_in_seconds):
    now = datetime.now() - timedelta(seconds=left)
    date_to = (now - timedelta(minutes=now.minute % 5)).replace(second=0, microsecond=0)
    list_time = [date_to - timedelta(seconds=timedelta_in_seconds), date_to]
    return list_time


def show_chart_1():
    df_pie = d.data.groupby('experience', as_index=False).id.count()
    x = df_pie.experience
    y = df_pie.id
    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot()
    ax.pie(y, labels=x, autopct='%1.1f%%', textprops={'fontsize': 11})
    return plt


def show_chart_2():
    data_b = d.salary_filter(d.data)
    data_b = data_b.pivot_table(index='id', columns='schedule', values='salary_mean')
    roles_list = ['Полный день', 'Удаленная работа', 'Гибкий график', 'Сменный график']
    data_list, list_name = d.li_na(data_b, roles_list)

    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot()
    bp = ax.boxplot(data_list, patch_artist=True)
    colors = ['ForestGreen', 'IndianRed', 'goldenrod', 'CadetBlue']

    valid_data_list = [data for data in data_list if len(data) > 1]
    medians = [np.median(data) for data in valid_data_list]

    for box, color, median, i in zip(bp['boxes'], colors, medians, range(1, len(valid_data_list) + 1)):
        box.set_facecolor(color)
        plt.setp(bp['medians'], linewidth=2)
        plt.text(i, median, f'{median}', va='bottom', ha="center", bbox=dict(facecolor="w", alpha=0.2))

    ax.ticklabel_format(style='plain', axis='y')
    ax.set_ylim(0, 580000)
    ax.set_title("Медианные предлагаемые зарплаты в ИТ-сфере в зависимости от графика работы")
    ax.set_yticks(range(0, 580000, 50000))
    ax.set_xticks(range(1, len(list_name) + 1))
    ax.set_xticklabels(list_name)
    ax.set_ylabel("Зарплата, руб.")
    ax.grid()


def show_chart_3():
    data_s = d.salary_filter(d.data)
    data_s = data_s.pivot_table(index='id', columns='experience', values='salary_mean')

    roles_list = ['Нет опыта', 'От 1 года до 3 лет', 'От 3 до 6 лет', 'Более 6 лет']
    data_list, list_name = d.li_na(data_s, roles_list)

    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot()
    bp = ax.boxplot(data_list, patch_artist=True)
    colors = ['ForestGreen', 'IndianRed', 'goldenrod', 'CadetBlue']

    valid_data_list = [data for data in data_list if len(data) > 1]
    medians = [np.median(data) for data in valid_data_list]

    for box, color, median, i in zip(bp['boxes'], colors, medians, range(1, len(valid_data_list) + 1)):
        box.set_facecolor(color)
        plt.setp(bp['medians'], linewidth=2)
        plt.text(i, median, f'{median}', va='bottom', ha="center", bbox=dict(facecolor="w", alpha=0.2))

    ax.ticklabel_format(style='plain', axis='y')
    ax.set_ylim(0, 580000)
    ax.set_title("Медианные предлагаемые зарплаты в ИТ-сфере в зависимости от опыта")
    ax.set_yticks(range(0, 580000, 50000))
    ax.set_xticks(range(1, len(list_name) + 1))
    ax.set_xticklabels(list_name)
    ax.set_ylabel("Зарплата, руб.")
    ax.grid()


def show_chart_4():
    data_area = d.salary_filter(d.data)
    are_list = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 'Владивосток', 'Казань',
                'Нижний Новгород', 'Ростов-на-Дону', 'Челябинск', 'Воронеж']
    data_area = data_area[data_area.area_name.isin(are_list) == True]
    data_area = data_area.pivot_table(index='id', columns='area_name', values='salary_mean')
    data_list, list_name = d.li_na(data_area, are_list)

    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot()
    bp = ax.boxplot(data_list, patch_artist=True)
    colors = ['ForestGreen', 'IndianRed', 'goldenrod', 'CadetBlue', 'RoyalBlue', 'MediumTurquoise', 'SeaGreen',
              'SkyBlue', 'DarkKhaki', 'Burlywood']

    valid_data_list = [data for data in data_list if len(data) > 1]
    medians = [np.median(data) for data in valid_data_list]

    for box, color, median, i in zip(bp['boxes'], colors, medians, range(1, len(valid_data_list) + 1)):
        box.set_facecolor(color)
        plt.setp(bp['medians'], linewidth=2)
        plt.text(i, median, f'{median}', va='bottom', ha="center", bbox=dict(facecolor="w", alpha=0.2))

    ax.set_ylim(0, 400000)
    ax.ticklabel_format(style='plain', axis='y')
    ax.set_xticks(range(1, len(list_name) + 1))
    ax.set_xticklabels(list_name, rotation=20)

    ax.set_yticks(range(0, 400000, 50000))
    ax.set_title("Медианные предлагаемые зарплаты по городам")
    ax.set_ylabel("Зарплата")
    ax.grid()


def show_chart_5():
    data_k = d.salary_filter(d.data)
    data_k = data_k.pivot_table(index='id', columns='employment', values='salary_mean')
    roles_list = ['Полная занятость', 'Частичная занятость', 'Стажировка', 'Проектная работа']
    data_list, list_name = d.li_na(data_k, roles_list)

    fig = plt.figure(figsize=(15, 10))
    ax = fig.add_subplot()
    bp = ax.boxplot(data_list, patch_artist=True)
    colors = ['ForestGreen', 'IndianRed', 'goldenrod', 'CadetBlue']

    valid_data_list = [data for data in data_list if len(data) > 1]
    medians = [np.median(data) for data in valid_data_list]

    for box, color, median, i in zip(bp['boxes'], colors, medians, range(1, len(valid_data_list) + 1)):
        box.set_facecolor(color)
        plt.setp(bp['medians'], linewidth=2)
        plt.text(i, median, f'{median}', va='bottom', ha="center", bbox=dict(facecolor="w", alpha=0.2))

    ax.ticklabel_format(style='plain', axis='y')
    ax.set_ylim(0, 580000)
    ax.set_title("Медианные предлагаемые зарплаты в ИТ-сфере в зависимости от опыта")
    ax.set_yticks(range(0, 580000, 50000))
    ax.set_xticks(range(1, len(list_name) + 1))
    ax.set_xticklabels(list_name)
    ax.set_ylabel("Зарплата, руб.")
    ax.grid()


def show_chart_6():
    data = d.data
    data_ex = data[(data.experience.isna() == False) & (data.schedule.isna() == False)]
    data_ex = data_ex[data_ex.professional_roles_name == 'Программист, разработчик']

    data_ex = data_ex.pivot_table(index='experience', columns='schedule', values='id', aggfunc='count', fill_value=0)
    x = data_ex.index
    y1 = data_ex['Полный день']
    y2 = data_ex['Удаленная работа']

    fig = plt.figure(figsize=(15, 10))
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)
    ax1.pie(y1, labels=x, autopct='%1.1f%%', textprops={'fontsize': 11})
    ax2.pie(y2, labels=x, autopct='%1.1f%%', textprops={'fontsize': 11})
    ax1.set_xlabel("Полный день", size=15)
    ax2.set_xlabel("Удаленная работа", size=15)


def build_graph(graph_type):
    if graph_type == "chart_1":
        show_chart_1()
    elif graph_type == "chart_2":
        show_chart_2()
    elif graph_type == "chart_3":
        show_chart_3()
    elif graph_type == "chart_4":
        show_chart_4()
    elif graph_type == "chart_5":
        show_chart_5()
    elif graph_type == "chart_6":
        show_chart_6()
    else:
        return None
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    image_png = buffer.getvalue()
    buffer.close()

    graph_url = base64.b64encode(image_png).decode('utf-8')
    return 'data:image/png;base64,' + graph_url


@app.route('/plot', methods=['POST'])
def plot():
    data = request.get_json()
    graph_type = data['graph']
    graph = build_graph(graph_type)
    return jsonify(graph)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
