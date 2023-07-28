### lesson 4 advanced metrics

Task:

В наших данных использования ленты новостей есть два типа юзеров: те, кто пришел через платный трафик source = 'ads', и те, кто пришел через органические каналы source = 'organic'.

Ваша задача — проанализировать и сравнить Retention этих двух групп пользователей. Решением этой задачи будет ответ на вопрос: отличается ли характер использования приложения у этих групп пользователей. 

В качестве ответа вы можете приложить ссылки на Superset, Redash или merge request в Gitlab, а также небольшое текстовое пояснение полученных результатов. 

Questions:
1. How retention looks for these two groups?
2. Is retention different in these two groups?
3. Is retention rate different?
4. extra. Are these groups otherwise similar? age, gender, os.
5. extra. survival rate analysis.

To do:
1. a. get a table: start_date, date, total_users, ads_users, organic_users, ads_users_percentage, organic_users_percentage, organic_users_percentage - ads_users_percentage
   b. get a table: user, start_date, active_days, last_date, os, gender, age_group, source.
2. plot Retention over time (heatmaps) for these two groups - compare by eye
3. plot Heatmap of % organic_left - % ads_left
4. chi square test for them
5. a/a test
6. 