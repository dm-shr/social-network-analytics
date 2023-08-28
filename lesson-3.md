### Lesson 3 - dashboards

#### Task 1a: analytical dashboad for feed
  1. Questions to answer:
  - How many: likes, views, users over time? groups with gender, os, source, country
  - Top: posts, users
  
  2. Metrics already plotted: 
  - likes, views, ctr by day over past month, 
  - unique users by day over past month,
  - dau, wau, mau,
  
  3. Columns in the feed:
   - user_id,
   - post_id,
   - action,
   - time,
   - gender,
   - age,
   - country,
   - city,
   - os,
   - source,
   - exp_group

  4. What to add more for past month/year analysis?
  - dau/mau (are there peak days wtih the same unique users as in the whole month?
<!--   - users by age group (young, middle, old) over time -->
<!--   - users by gender over time -->
<!--   - users by source and os over iime, their activity over time -->
<!--   - postings over time -->
<!--   - metrics per user - likes, views, postings -->
<!--   - top: top posters, top likers, top viewers, top viewed posts and liked posts -->
  

#### Task 1b: operational dashboad for feed

  1. Questions to answer:

  
  2. Metrics already plotted: 

  
  3. Columns in the feed:
   - user_id,
   - post_id,
   - action,
   - time,
   - gender,
   - age,
   - country,
   - city,
   - os,
   - source,
   - exp_group

  4. What to add more?


#### Task 2: connection between the feed and the messager

  1. Questions to answer:
  
     1. Какая у нашего приложения активная аудитория по дням, т.е. пользователи, которые пользуются и лентой новостей, и сервисом сообщений. 
     
what do we look at here?

    -- how many unique active users daily?
    -- are they more "active" in feed than non-active users? look at likes in feed
    -- does their source differ from non-active?
    -- any gender/age group difference?
    
    
how to solve?

a) date to day truncate for feed and message tables. 
    feed: group by user, day - count unique views, likes. - done
    message: select distinct user, day - done
    
b) do full join for modfied feed (user, day) and message (user, day) tables on "user" and "day" - done

c) to the result from b), add a new columnn day_activity_type: if both days present, set both, else "feed" or "message" - done

d) take table from c) (user, day, day_act_type), left join it with modified feed (user, day, unique views, likes) on user and day. empty views, likes - fill with zero

e) take table from d) (user, day, type, views, likes) and left join with feed (user, user params like gender etc) on user

f) finally, we have a table:

   - user_id,
   - day,
   - day_activity_type,
   - unique_views,
   - likes,
   - gender,
   - age,
   - age_group,
   - country,
   - city,
   - os,
   - source,
   - exp_group
   
Possible problems here:
1. there can be users with different metadata dependent on the day: city can vary or country or os
checked for feed - no such users. for message - same! good

     2. Сколько пользователей использует только ленту новостей и не пользуются сообщениями.   

  
  2. Metrics already plotted: 

  
  3. Columns in the feed:
   - user_id,
   - post_id,
   - action,
   - time,
   - gender,
   - age,
   - country,
   - city,
   - os,
   - source,
   - exp_group
   
       SELECT user_id,
           reciever_id,
           time,
           source,
           exp_group,
           gender,
           age,
           country,
           city,
           os
    FROM simulator_20230720.message_actions
    LIMIT 100
    
 

  4. What to add more?
