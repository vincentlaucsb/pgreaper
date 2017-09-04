Benchmark/Example: pandas DataFrame COPY and UPSERT
====================================================

COPY
-----
Here, we're going to use the excellent `mimesis` package to generate 500,000
rows of fake data to populate a pandas DataFrame.

.. note:: For robustness, PGReaper uses both fake and real data in its test suite.

.. code:: ipython3

    from pandas import DataFrame
    from pgreaper import copy_df
    import pandas
    
    # Generate Fake Info
    import mimesis
    import random

.. code:: ipython3

    # Generate a CSV with information for 500,000 fake persons
    
    person = mimesis.Personal(locale='en')
    rows = []
    header = ['id', 'Full Name', 'Age', 'Occupation', 'Contact', 'Nationality']
        
    for i in range(0, 500000):
        rando = random.uniform(0, 1)
        if rando >= 0.5:
            gender = 'female'
        else:
            gender = 'male'
    
        contact = {
            'email': person.email(gender=gender),
            'phone': person.telephone(),
        }
    
        row = [
            i,
            person.full_name(gender=gender),
            person.age(),
            person.occupation(),
            contact,
            person.nationality()
        ]
        rows.append(row)

Structure
"""""""""""
The resulting SQL table should have 4 text, 1 bigint, and 1 jsonb column.

.. code:: ipython3

    persons = pandas.DataFrame(
        columns=header,
        data=rows,
    )

.. code:: ipython3

    persons




.. raw:: html

    <div>
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>id</th>
          <th>Full Name</th>
          <th>Age</th>
          <th>Occupation</th>
          <th>Contact</th>
          <th>Nationality</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>0</td>
          <td>Jimmy Vaughn</td>
          <td>25</td>
          <td>Leather Worker</td>
          <td>{'email': 'inell_8213@yandex.com', 'phone': '(...</td>
          <td>Uruguayan</td>
        </tr>
        <tr>
          <th>1</th>
          <td>1</td>
          <td>Douglas Noel</td>
          <td>50</td>
          <td>Bricklayer</td>
          <td>{'email': 'carter-3572@outlook.com', 'phone': ...</td>
          <td>Uruguayan</td>
        </tr>
        <tr>
          <th>2</th>
          <td>2</td>
          <td>Aaron Figueroa</td>
          <td>38</td>
          <td>Bank Messenger</td>
          <td>{'email': 'leopoldo9374@live.com', 'phone': '0...</td>
          <td>Russian</td>
        </tr>
        <tr>
          <th>3</th>
          <td>3</td>
          <td>Lonnie Rose</td>
          <td>57</td>
          <td>Merchant Seaman</td>
          <td>{'email': 'dudley-6367@live.com', 'phone': '1-...</td>
          <td>Romanian</td>
        </tr>
        <tr>
          <th>4</th>
          <td>4</td>
          <td>Mafalda Gross</td>
          <td>43</td>
          <td>Song Writer</td>
          <td>{'email': 'magdalen_3735@outlook.com', 'phone'...</td>
          <td>Swiss</td>
        </tr>
        <tr>
          <th>5</th>
          <td>5</td>
          <td>Cammie Kirkland</td>
          <td>55</td>
          <td>Flour Miller</td>
          <td>{'email': 'lorrine_2944@yahoo.com', 'phone': '...</td>
          <td>Puerto Rican</td>
        </tr>
        <tr>
          <th>6</th>
          <td>6</td>
          <td>Alfonzo Harper</td>
          <td>31</td>
          <td>Employee</td>
          <td>{'email': 'len_3927@gmail.com', 'phone': '(068...</td>
          <td>Australian</td>
        </tr>
        <tr>
          <th>7</th>
          <td>7</td>
          <td>Vaughn Herman</td>
          <td>21</td>
          <td>Stage Hand</td>
          <td>{'email': 'federico_3662@live.com', 'phone': '...</td>
          <td>Salvadorian</td>
        </tr>
        <tr>
          <th>8</th>
          <td>8</td>
          <td>My Hendricks</td>
          <td>58</td>
          <td>Mining Engineer</td>
          <td>{'email': 'my_9446@gmail.com', 'phone': '068-8...</td>
          <td>Finnish</td>
        </tr>
        <tr>
          <th>9</th>
          <td>9</td>
          <td>Moses Moran</td>
          <td>58</td>
          <td>Sheriff Clerk</td>
          <td>{'email': 'delmer-761@live.com', 'phone': '+1-...</td>
          <td>British</td>
        </tr>
        <tr>
          <th>10</th>
          <td>10</td>
          <td>Hyman Leach</td>
          <td>20</td>
          <td>Painter</td>
          <td>{'email': 'reinaldo-2219@live.com', 'phone': '...</td>
          <td>Australian</td>
        </tr>
        <tr>
          <th>11</th>
          <td>11</td>
          <td>Elsy Ball</td>
          <td>50</td>
          <td>Records Supervisor</td>
          <td>{'email': 'tu_847@live.com', 'phone': '(068) 8...</td>
          <td>Bolivian</td>
        </tr>
        <tr>
          <th>12</th>
          <td>12</td>
          <td>Crista Washington</td>
          <td>45</td>
          <td>Homeopath</td>
          <td>{'email': 'lady9072@outlook.com', 'phone': '06...</td>
          <td>Greek</td>
        </tr>
        <tr>
          <th>13</th>
          <td>13</td>
          <td>Matthew Shaw</td>
          <td>63</td>
          <td>Machine Fitters</td>
          <td>{'email': 'sol_3090@yahoo.com', 'phone': '1-06...</td>
          <td>Jordanian</td>
        </tr>
        <tr>
          <th>14</th>
          <td>14</td>
          <td>Versie Stephens</td>
          <td>56</td>
          <td>Underwriter</td>
          <td>{'email': 'maragret-9589@live.com', 'phone': '...</td>
          <td>Cambodian</td>
        </tr>
        <tr>
          <th>15</th>
          <td>15</td>
          <td>Herb Gonzales</td>
          <td>63</td>
          <td>Sign Maker</td>
          <td>{'email': 'elisha-258@outlook.com', 'phone': '...</td>
          <td>Egyptian</td>
        </tr>
        <tr>
          <th>16</th>
          <td>16</td>
          <td>Jerrod Peterson</td>
          <td>38</td>
          <td>Vehicle Engineer</td>
          <td>{'email': 'len-8721@gmail.com', 'phone': '1-06...</td>
          <td>Australian</td>
        </tr>
        <tr>
          <th>17</th>
          <td>17</td>
          <td>Randal Wyatt</td>
          <td>50</td>
          <td>Purchase Clerk</td>
          <td>{'email': 'mack-9125@yandex.com', 'phone': '06...</td>
          <td>Puerto Rican</td>
        </tr>
        <tr>
          <th>18</th>
          <td>18</td>
          <td>Sabine Powell</td>
          <td>42</td>
          <td>Buyer</td>
          <td>{'email': 'lizzette-8591@live.com', 'phone': '...</td>
          <td>Ukrainian</td>
        </tr>
        <tr>
          <th>19</th>
          <td>19</td>
          <td>Ulrike Wyatt</td>
          <td>29</td>
          <td>Stonemason</td>
          <td>{'email': 'pandora9731@outlook.com', 'phone': ...</td>
          <td>Swiss</td>
        </tr>
        <tr>
          <th>20</th>
          <td>20</td>
          <td>Emmie Hartman</td>
          <td>36</td>
          <td>Market Trader</td>
          <td>{'email': 'eusebia9143@yandex.com', 'phone': '...</td>
          <td>Egyptian</td>
        </tr>
        <tr>
          <th>21</th>
          <td>21</td>
          <td>Liberty Willis</td>
          <td>38</td>
          <td>Technical Author</td>
          <td>{'email': 'pearlene8013@yandex.com', 'phone': ...</td>
          <td>Puerto Rican</td>
        </tr>
        <tr>
          <th>22</th>
          <td>22</td>
          <td>Teddy Weaver</td>
          <td>35</td>
          <td>Materials Controller</td>
          <td>{'email': 'frederic-2350@yandex.com', 'phone':...</td>
          <td>Dominican</td>
        </tr>
        <tr>
          <th>23</th>
          <td>23</td>
          <td>Maddie Malone</td>
          <td>61</td>
          <td>Stone Sawyer</td>
          <td>{'email': 'miki2798@gmail.com', 'phone': '1-06...</td>
          <td>Finnish</td>
        </tr>
        <tr>
          <th>24</th>
          <td>24</td>
          <td>Olevia Mcdaniel</td>
          <td>22</td>
          <td>Playgroup Leader</td>
          <td>{'email': 'retta-1501@yahoo.com', 'phone': '06...</td>
          <td>Australian</td>
        </tr>
        <tr>
          <th>25</th>
          <td>25</td>
          <td>Tinisha Christian</td>
          <td>64</td>
          <td>Assessor</td>
          <td>{'email': 'amina_814@outlook.com', 'phone': '0...</td>
          <td>Argentinian</td>
        </tr>
        <tr>
          <th>26</th>
          <td>26</td>
          <td>Tova Sanchez</td>
          <td>51</td>
          <td>Assistant Nurse</td>
          <td>{'email': 'pei-4002@live.com', 'phone': '068.8...</td>
          <td>Mexican</td>
        </tr>
        <tr>
          <th>27</th>
          <td>27</td>
          <td>Theo Williamson</td>
          <td>30</td>
          <td>Research Technician</td>
          <td>{'email': 'lorenzo_6203@yandex.com', 'phone': ...</td>
          <td>Japanese</td>
        </tr>
        <tr>
          <th>28</th>
          <td>28</td>
          <td>Maurice Payne</td>
          <td>50</td>
          <td>Health Nurse</td>
          <td>{'email': 'alease6289@gmail.com', 'phone': '06...</td>
          <td>Taiwanese</td>
        </tr>
        <tr>
          <th>29</th>
          <td>29</td>
          <td>Paulita Hughes</td>
          <td>38</td>
          <td>Investment Strategist</td>
          <td>{'email': 'oneida9325@outlook.com', 'phone': '...</td>
          <td>Ethiopian</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>499970</th>
          <td>499970</td>
          <td>Wilford Rowe</td>
          <td>35</td>
          <td>Trade Union Official</td>
          <td>{'email': 'burl9662@live.com', 'phone': '628-8...</td>
          <td>Estonian</td>
        </tr>
        <tr>
          <th>499971</th>
          <td>499971</td>
          <td>Arie Summers</td>
          <td>25</td>
          <td>Special Needs</td>
          <td>{'email': 'carlee7311@gmail.com', 'phone': '62...</td>
          <td>Irish</td>
        </tr>
        <tr>
          <th>499972</th>
          <td>499972</td>
          <td>Foster Briggs</td>
          <td>20</td>
          <td>Taxi Controller</td>
          <td>{'email': 'homer2132@gmail.com', 'phone': '+1-...</td>
          <td>Cuban</td>
        </tr>
        <tr>
          <th>499973</th>
          <td>499973</td>
          <td>Korey Pugh</td>
          <td>66</td>
          <td>Decorator</td>
          <td>{'email': 'mathew1418@gmail.com', 'phone': '62...</td>
          <td>Dominican</td>
        </tr>
        <tr>
          <th>499974</th>
          <td>499974</td>
          <td>Berry Solis</td>
          <td>27</td>
          <td>Waiter</td>
          <td>{'email': 'lory6921@gmail.com', 'phone': '1-62...</td>
          <td>Puerto Rican</td>
        </tr>
        <tr>
          <th>499975</th>
          <td>499975</td>
          <td>Tequila William</td>
          <td>38</td>
          <td>Sand Blaster</td>
          <td>{'email': 'karly-9354@live.com', 'phone': '(62...</td>
          <td>Chilean</td>
        </tr>
        <tr>
          <th>499976</th>
          <td>499976</td>
          <td>Meridith Wright</td>
          <td>59</td>
          <td>Genealogist</td>
          <td>{'email': 'cyndy-8184@outlook.com', 'phone': '...</td>
          <td>Canadian</td>
        </tr>
        <tr>
          <th>499977</th>
          <td>499977</td>
          <td>Jacqui Serrano</td>
          <td>51</td>
          <td>Tyre Builder</td>
          <td>{'email': 'mercedez_8467@yahoo.com', 'phone': ...</td>
          <td>Cameroonian</td>
        </tr>
        <tr>
          <th>499978</th>
          <td>499978</td>
          <td>Clemente Powell</td>
          <td>22</td>
          <td>Music Teacher</td>
          <td>{'email': 'carmen_5259@gmail.com', 'phone': '3...</td>
          <td>Australian</td>
        </tr>
        <tr>
          <th>499979</th>
          <td>499979</td>
          <td>Will Hale</td>
          <td>22</td>
          <td>Radio Presenter</td>
          <td>{'email': 'jamison_5100@live.com', 'phone': '1...</td>
          <td>Cuban</td>
        </tr>
        <tr>
          <th>499980</th>
          <td>499980</td>
          <td>Lester Butler</td>
          <td>63</td>
          <td>Sportswoman</td>
          <td>{'email': 'william7285@live.com', 'phone': '1-...</td>
          <td>Colombian</td>
        </tr>
        <tr>
          <th>499981</th>
          <td>499981</td>
          <td>Lianne Irwin</td>
          <td>47</td>
          <td>Applications Engineer</td>
          <td>{'email': 'madison-7241@yahoo.com', 'phone': '...</td>
          <td>Puerto Rican</td>
        </tr>
        <tr>
          <th>499982</th>
          <td>499982</td>
          <td>Mistie Medina</td>
          <td>47</td>
          <td>Health Advisor</td>
          <td>{'email': 'valene8224@outlook.com', 'phone': '...</td>
          <td>Chinese</td>
        </tr>
        <tr>
          <th>499983</th>
          <td>499983</td>
          <td>Keven Beck</td>
          <td>41</td>
          <td>Tyre Fitter</td>
          <td>{'email': 'noel_2584@live.com', 'phone': '(981...</td>
          <td>Argentinian</td>
        </tr>
        <tr>
          <th>499984</th>
          <td>499984</td>
          <td>Tyler Beasley</td>
          <td>36</td>
          <td>School Inspector</td>
          <td>{'email': 'antony2949@yandex.com', 'phone': '1...</td>
          <td>Bolivian</td>
        </tr>
        <tr>
          <th>499985</th>
          <td>499985</td>
          <td>Myung Sanford</td>
          <td>46</td>
          <td>Production Planner</td>
          <td>{'email': 'naida_7091@yahoo.com', 'phone': '98...</td>
          <td>Chilean</td>
        </tr>
        <tr>
          <th>499986</th>
          <td>499986</td>
          <td>Esteban Lowe</td>
          <td>20</td>
          <td>Radiologist</td>
          <td>{'email': 'williams_7486@yahoo.com', 'phone': ...</td>
          <td>Mexican</td>
        </tr>
        <tr>
          <th>499987</th>
          <td>499987</td>
          <td>Leia Cunningham</td>
          <td>49</td>
          <td>Orthoptist</td>
          <td>{'email': 'marcell9206@yahoo.com', 'phone': '1...</td>
          <td>Costa Rican</td>
        </tr>
        <tr>
          <th>499988</th>
          <td>499988</td>
          <td>Elbert Rodriquez</td>
          <td>17</td>
          <td>Catering Staff</td>
          <td>{'email': 'gregg-7474@gmail.com', 'phone': '96...</td>
          <td>Ethiopian</td>
        </tr>
        <tr>
          <th>499989</th>
          <td>499989</td>
          <td>Meri Mathews</td>
          <td>59</td>
          <td>Sales Executive</td>
          <td>{'email': 'thomasena5180@live.com', 'phone': '...</td>
          <td>Greek</td>
        </tr>
        <tr>
          <th>499990</th>
          <td>499990</td>
          <td>Ron Velasquez</td>
          <td>45</td>
          <td>Security Officer</td>
          <td>{'email': 'dan1402@yahoo.com', 'phone': '(963)...</td>
          <td>Belgian</td>
        </tr>
        <tr>
          <th>499991</th>
          <td>499991</td>
          <td>Adolfo Hickman</td>
          <td>35</td>
          <td>Professional Wrestler</td>
          <td>{'email': 'blake-8646@live.com', 'phone': '(96...</td>
          <td>Australian</td>
        </tr>
        <tr>
          <th>499992</th>
          <td>499992</td>
          <td>Pearle Dotson</td>
          <td>20</td>
          <td>Seamstress</td>
          <td>{'email': 'theo_1294@yahoo.com', 'phone': '1-9...</td>
          <td>Salvadorian</td>
        </tr>
        <tr>
          <th>499993</th>
          <td>499993</td>
          <td>Stefania Mays</td>
          <td>28</td>
          <td>Party Planner</td>
          <td>{'email': 'evan_3309@yandex.com', 'phone': '28...</td>
          <td>Afghan</td>
        </tr>
        <tr>
          <th>499994</th>
          <td>499994</td>
          <td>Luis Bond</td>
          <td>29</td>
          <td>Area Manager</td>
          <td>{'email': 'eduardo-2172@outlook.com', 'phone':...</td>
          <td>French</td>
        </tr>
        <tr>
          <th>499995</th>
          <td>499995</td>
          <td>Irina Gibbs</td>
          <td>64</td>
          <td>History Teacher</td>
          <td>{'email': 'delmy_8959@outlook.com', 'phone': '...</td>
          <td>Italian</td>
        </tr>
        <tr>
          <th>499996</th>
          <td>499996</td>
          <td>Emery Anderson</td>
          <td>59</td>
          <td>Chambermaid</td>
          <td>{'email': 'percy6103@live.com', 'phone': '288....</td>
          <td>Spanish</td>
        </tr>
        <tr>
          <th>499997</th>
          <td>499997</td>
          <td>Camie Frazier</td>
          <td>38</td>
          <td>Technical Liaison</td>
          <td>{'email': 'emelda-127@outlook.com', 'phone': '...</td>
          <td>British</td>
        </tr>
        <tr>
          <th>499998</th>
          <td>499998</td>
          <td>Jospeh Reid</td>
          <td>26</td>
          <td>Historian</td>
          <td>{'email': 'ferdinand_5862@yandex.com', 'phone'...</td>
          <td>Guatemalan</td>
        </tr>
        <tr>
          <th>499999</th>
          <td>499999</td>
          <td>Argelia Payne</td>
          <td>35</td>
          <td>Station Manager</td>
          <td>{'email': 'domonique5565@gmail.com', 'phone': ...</td>
          <td>Australian</td>
        </tr>
      </tbody>
    </table>
    <p>500000 rows × 6 columns</p>
    </div>


Results
""""""""
And for the moment of truth...    
    
.. code:: ipython3

    %%timeit -n1 -r1
    copy_df(persons, name='persons', p_key='id', dbname='pgreaper_test')


.. parsed-literal::

    1 loop, best of 1: 10.7 s per loop
    

UPSERT
-------
Suppose now that we live in such an amazing economy that everybody past 50 
has enough money to retire. This means we'll need to update our data to reflect this. As you can see for yourself, this operation will affect about 160,000 rows.
    
.. code:: ipython3

    %load_ext sql
    
    import pandas
    import pgreaper

.. code:: ipython3

    retired = pandas.read_sql(
        sql='SELECT * FROM persons WHERE age >= 50',
        con=pgreaper.PG_DEFAULTS.to_string(dbname='pgreaper_test'))

.. code:: ipython3

    retired['occupation'] = 'Retired'
    
.. code:: ipython3

    retired




.. raw:: html

    <div>
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>id</th>
          <th>full_name</th>
          <th>age</th>
          <th>occupation</th>
          <th>contact</th>
          <th>nationality</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>1</td>
          <td>Douglas Noel</td>
          <td>50</td>
          <td>Retired</td>
          <td>{'email': 'carter-3572@outlook.com', 'phone': ...</td>
          <td>Uruguayan</td>
        </tr>
        <tr>
          <th>1</th>
          <td>3</td>
          <td>Lonnie Rose</td>
          <td>57</td>
          <td>Retired</td>
          <td>{'email': 'dudley-6367@live.com', 'phone': '1-...</td>
          <td>Romanian</td>
        </tr>
        <tr>
          <th>2</th>
          <td>5</td>
          <td>Cammie Kirkland</td>
          <td>55</td>
          <td>Retired</td>
          <td>{'email': 'lorrine_2944@yahoo.com', 'phone': '...</td>
          <td>Puerto Rican</td>
        </tr>
        <tr>
          <th>3</th>
          <td>8</td>
          <td>My Hendricks</td>
          <td>58</td>
          <td>Retired</td>
          <td>{'email': 'my_9446@gmail.com', 'phone': '068-8...</td>
          <td>Finnish</td>
        </tr>
        <tr>
          <th>4</th>
          <td>9</td>
          <td>Moses Moran</td>
          <td>58</td>
          <td>Retired</td>
          <td>{'email': 'delmer-761@live.com', 'phone': '+1-...</td>
          <td>British</td>
        </tr>
        <tr>
          <th>5</th>
          <td>11</td>
          <td>Elsy Ball</td>
          <td>50</td>
          <td>Retired</td>
          <td>{'email': 'tu_847@live.com', 'phone': '(068) 8...</td>
          <td>Bolivian</td>
        </tr>
        <tr>
          <th>6</th>
          <td>13</td>
          <td>Matthew Shaw</td>
          <td>63</td>
          <td>Retired</td>
          <td>{'email': 'sol_3090@yahoo.com', 'phone': '1-06...</td>
          <td>Jordanian</td>
        </tr>
        <tr>
          <th>7</th>
          <td>14</td>
          <td>Versie Stephens</td>
          <td>56</td>
          <td>Retired</td>
          <td>{'email': 'maragret-9589@live.com', 'phone': '...</td>
          <td>Cambodian</td>
        </tr>
        <tr>
          <th>8</th>
          <td>15</td>
          <td>Herb Gonzales</td>
          <td>63</td>
          <td>Retired</td>
          <td>{'email': 'elisha-258@outlook.com', 'phone': '...</td>
          <td>Egyptian</td>
        </tr>
        <tr>
          <th>9</th>
          <td>17</td>
          <td>Randal Wyatt</td>
          <td>50</td>
          <td>Retired</td>
          <td>{'email': 'mack-9125@yandex.com', 'phone': '06...</td>
          <td>Puerto Rican</td>
        </tr>
        <tr>
          <th>10</th>
          <td>23</td>
          <td>Maddie Malone</td>
          <td>61</td>
          <td>Retired</td>
          <td>{'email': 'miki2798@gmail.com', 'phone': '1-06...</td>
          <td>Finnish</td>
        </tr>
        <tr>
          <th>11</th>
          <td>25</td>
          <td>Tinisha Christian</td>
          <td>64</td>
          <td>Retired</td>
          <td>{'email': 'amina_814@outlook.com', 'phone': '0...</td>
          <td>Argentinian</td>
        </tr>
        <tr>
          <th>12</th>
          <td>26</td>
          <td>Tova Sanchez</td>
          <td>51</td>
          <td>Retired</td>
          <td>{'email': 'pei-4002@live.com', 'phone': '068.8...</td>
          <td>Mexican</td>
        </tr>
        <tr>
          <th>13</th>
          <td>28</td>
          <td>Maurice Payne</td>
          <td>50</td>
          <td>Retired</td>
          <td>{'email': 'alease6289@gmail.com', 'phone': '06...</td>
          <td>Taiwanese</td>
        </tr>
        <tr>
          <th>14</th>
          <td>32</td>
          <td>Delmer Saunders</td>
          <td>52</td>
          <td>Retired</td>
          <td>{'email': 'lorenzo-4738@yandex.com', 'phone': ...</td>
          <td>German</td>
        </tr>
        <tr>
          <th>15</th>
          <td>33</td>
          <td>Kit Holcomb</td>
          <td>50</td>
          <td>Retired</td>
          <td>{'email': 'ladawn8977@outlook.com', 'phone': '...</td>
          <td>Latvian</td>
        </tr>
        <tr>
          <th>16</th>
          <td>36</td>
          <td>Dylan Burgess</td>
          <td>56</td>
          <td>Retired</td>
          <td>{'email': 'cliff_228@live.com', 'phone': '068....</td>
          <td>Argentinian</td>
        </tr>
        <tr>
          <th>17</th>
          <td>38</td>
          <td>Francisco Wiley</td>
          <td>55</td>
          <td>Retired</td>
          <td>{'email': 'georgine_4373@yandex.com', 'phone':...</td>
          <td>Cambodian</td>
        </tr>
        <tr>
          <th>18</th>
          <td>39</td>
          <td>Stuart Hendricks</td>
          <td>66</td>
          <td>Retired</td>
          <td>{'email': 'andre_9255@live.com', 'phone': '068...</td>
          <td>Romanian</td>
        </tr>
        <tr>
          <th>19</th>
          <td>41</td>
          <td>Gerry Holt</td>
          <td>62</td>
          <td>Retired</td>
          <td>{'email': 'kyle6356@outlook.com', 'phone': '06...</td>
          <td>Chilean</td>
        </tr>
        <tr>
          <th>20</th>
          <td>46</td>
          <td>Rosio Henson</td>
          <td>58</td>
          <td>Retired</td>
          <td>{'email': 'marielle9323@yahoo.com', 'phone': '...</td>
          <td>Afghan</td>
        </tr>
        <tr>
          <th>21</th>
          <td>49</td>
          <td>Archie Vega</td>
          <td>62</td>
          <td>Retired</td>
          <td>{'email': 'timothy-7344@outlook.com', 'phone':...</td>
          <td>Brazilian</td>
        </tr>
        <tr>
          <th>22</th>
          <td>51</td>
          <td>Dudley Richmond</td>
          <td>55</td>
          <td>Retired</td>
          <td>{'email': 'bob_7237@yandex.com', 'phone': '(06...</td>
          <td>Cuban</td>
        </tr>
        <tr>
          <th>23</th>
          <td>52</td>
          <td>Harley Matthews</td>
          <td>58</td>
          <td>Retired</td>
          <td>{'email': 'albert_9262@live.com', 'phone': '06...</td>
          <td>Uruguayan</td>
        </tr>
        <tr>
          <th>24</th>
          <td>53</td>
          <td>Blair Blake</td>
          <td>61</td>
          <td>Retired</td>
          <td>{'email': 'kenton-562@outlook.com', 'phone': '...</td>
          <td>Dominican</td>
        </tr>
        <tr>
          <th>25</th>
          <td>56</td>
          <td>Kelly Logan</td>
          <td>66</td>
          <td>Retired</td>
          <td>{'email': 'hubert-9681@live.com', 'phone': '+1...</td>
          <td>Polish</td>
        </tr>
        <tr>
          <th>26</th>
          <td>57</td>
          <td>Raymon Flowers</td>
          <td>62</td>
          <td>Retired</td>
          <td>{'email': 'jonathon-8669@outlook.com', 'phone'...</td>
          <td>Swiss</td>
        </tr>
        <tr>
          <th>27</th>
          <td>58</td>
          <td>Vertie Cochran</td>
          <td>64</td>
          <td>Retired</td>
          <td>{'email': 'vincenza-649@outlook.com', 'phone':...</td>
          <td>Portuguese</td>
        </tr>
        <tr>
          <th>28</th>
          <td>59</td>
          <td>Stacy Reed</td>
          <td>62</td>
          <td>Retired</td>
          <td>{'email': 'keith6772@gmail.com', 'phone': '(06...</td>
          <td>Chilean</td>
        </tr>
        <tr>
          <th>29</th>
          <td>64</td>
          <td>Delbert Emerson</td>
          <td>50</td>
          <td>Retired</td>
          <td>{'email': 'kraig-4725@outlook.com', 'phone': '...</td>
          <td>French</td>
        </tr>
        <tr>
          <th>...</th>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
          <td>...</td>
        </tr>
        <tr>
          <th>166595</th>
          <td>499896</td>
          <td>Argelia Robles</td>
          <td>53</td>
          <td>Retired</td>
          <td>{'email': 'hildred_2878@yahoo.com', 'phone': '...</td>
          <td>Egyptian</td>
        </tr>
        <tr>
          <th>166596</th>
          <td>499904</td>
          <td>Janean Marshall</td>
          <td>61</td>
          <td>Retired</td>
          <td>{'email': 'shawnee994@yahoo.com', 'phone': '93...</td>
          <td>Danish</td>
        </tr>
        <tr>
          <th>166597</th>
          <td>499906</td>
          <td>Sunday Morgan</td>
          <td>61</td>
          <td>Retired</td>
          <td>{'email': 'wesley_648@live.com', 'phone': '533...</td>
          <td>Cuban</td>
        </tr>
        <tr>
          <th>166598</th>
          <td>499909</td>
          <td>Marty Cross</td>
          <td>61</td>
          <td>Retired</td>
          <td>{'email': 'brooks_9153@yahoo.com', 'phone': '5...</td>
          <td>Israeli</td>
        </tr>
        <tr>
          <th>166599</th>
          <td>499912</td>
          <td>Kiana Abbott</td>
          <td>57</td>
          <td>Retired</td>
          <td>{'email': 'charita-9682@live.com', 'phone': '7...</td>
          <td>Bolivian</td>
        </tr>
        <tr>
          <th>166600</th>
          <td>499913</td>
          <td>Thomasena Fowler</td>
          <td>52</td>
          <td>Retired</td>
          <td>{'email': 'mitzie_7093@outlook.com', 'phone': ...</td>
          <td>Chilean</td>
        </tr>
        <tr>
          <th>166601</th>
          <td>499914</td>
          <td>Hobert Alford</td>
          <td>61</td>
          <td>Retired</td>
          <td>{'email': 'chuck2797@live.com', 'phone': '1-70...</td>
          <td>Swiss</td>
        </tr>
        <tr>
          <th>166602</th>
          <td>499919</td>
          <td>Brice Arnold</td>
          <td>63</td>
          <td>Retired</td>
          <td>{'email': 'malik_529@yandex.com', 'phone': '48...</td>
          <td>Russian</td>
        </tr>
        <tr>
          <th>166603</th>
          <td>499921</td>
          <td>Marcus Pearson</td>
          <td>56</td>
          <td>Retired</td>
          <td>{'email': 'rickie-7134@outlook.com', 'phone': ...</td>
          <td>Dutch</td>
        </tr>
        <tr>
          <th>166604</th>
          <td>499923</td>
          <td>Gricelda Dillon</td>
          <td>65</td>
          <td>Retired</td>
          <td>{'email': 'galina_1993@gmail.com', 'phone': '5...</td>
          <td>Venezuelan</td>
        </tr>
        <tr>
          <th>166605</th>
          <td>499925</td>
          <td>Idell Hopper</td>
          <td>51</td>
          <td>Retired</td>
          <td>{'email': 'berenice9972@gmail.com', 'phone': '...</td>
          <td>English</td>
        </tr>
        <tr>
          <th>166606</th>
          <td>499930</td>
          <td>Francesco Anthony</td>
          <td>58</td>
          <td>Retired</td>
          <td>{'email': 'rex_2601@outlook.com', 'phone': '61...</td>
          <td>Chilean</td>
        </tr>
        <tr>
          <th>166607</th>
          <td>499933</td>
          <td>Elroy Morton</td>
          <td>58</td>
          <td>Retired</td>
          <td>{'email': 'brad4797@live.com', 'phone': '1-830...</td>
          <td>Jordanian</td>
        </tr>
        <tr>
          <th>166608</th>
          <td>499935</td>
          <td>Rosette Giles</td>
          <td>66</td>
          <td>Retired</td>
          <td>{'email': 'lahoma-857@outlook.com', 'phone': '...</td>
          <td>Canadian</td>
        </tr>
        <tr>
          <th>166609</th>
          <td>499938</td>
          <td>Art Charles</td>
          <td>52</td>
          <td>Retired</td>
          <td>{'email': 'dustin1542@live.com', 'phone': '074...</td>
          <td>Brazilian</td>
        </tr>
        <tr>
          <th>166610</th>
          <td>499941</td>
          <td>Dong Reyes</td>
          <td>65</td>
          <td>Retired</td>
          <td>{'email': 'krystin-4097@yahoo.com', 'phone': '...</td>
          <td>Swiss</td>
        </tr>
        <tr>
          <th>166611</th>
          <td>499942</td>
          <td>Gino Dalton</td>
          <td>61</td>
          <td>Retired</td>
          <td>{'email': 'henry_5319@live.com', 'phone': '074...</td>
          <td>Guatemalan</td>
        </tr>
        <tr>
          <th>166612</th>
          <td>499949</td>
          <td>Marshall White</td>
          <td>58</td>
          <td>Retired</td>
          <td>{'email': 'aliza-7392@yandex.com', 'phone': '1...</td>
          <td>Swedish</td>
        </tr>
        <tr>
          <th>166613</th>
          <td>499954</td>
          <td>Rudy Gill</td>
          <td>50</td>
          <td>Retired</td>
          <td>{'email': 'rudolph8986@yahoo.com', 'phone': '(...</td>
          <td>Brazilian</td>
        </tr>
        <tr>
          <th>166614</th>
          <td>499955</td>
          <td>Rhona Hubbard</td>
          <td>59</td>
          <td>Retired</td>
          <td>{'email': 'chau_5207@yandex.com', 'phone': '49...</td>
          <td>Puerto Rican</td>
        </tr>
        <tr>
          <th>166615</th>
          <td>499965</td>
          <td>Kenny Best</td>
          <td>59</td>
          <td>Retired</td>
          <td>{'email': 'chauncey-5684@gmail.com', 'phone': ...</td>
          <td>Polish</td>
        </tr>
        <tr>
          <th>166616</th>
          <td>499968</td>
          <td>Madalene Yates</td>
          <td>58</td>
          <td>Retired</td>
          <td>{'email': 'kenisha604@gmail.com', 'phone': '06...</td>
          <td>British</td>
        </tr>
        <tr>
          <th>166617</th>
          <td>499969</td>
          <td>Beata Pugh</td>
          <td>52</td>
          <td>Retired</td>
          <td>{'email': 'kit-2831@outlook.com', 'phone': '06...</td>
          <td>German</td>
        </tr>
        <tr>
          <th>166618</th>
          <td>499973</td>
          <td>Korey Pugh</td>
          <td>66</td>
          <td>Retired</td>
          <td>{'email': 'mathew1418@gmail.com', 'phone': '62...</td>
          <td>Dominican</td>
        </tr>
        <tr>
          <th>166619</th>
          <td>499976</td>
          <td>Meridith Wright</td>
          <td>59</td>
          <td>Retired</td>
          <td>{'email': 'cyndy-8184@outlook.com', 'phone': '...</td>
          <td>Canadian</td>
        </tr>
        <tr>
          <th>166620</th>
          <td>499977</td>
          <td>Jacqui Serrano</td>
          <td>51</td>
          <td>Retired</td>
          <td>{'email': 'mercedez_8467@yahoo.com', 'phone': ...</td>
          <td>Cameroonian</td>
        </tr>
        <tr>
          <th>166621</th>
          <td>499980</td>
          <td>Lester Butler</td>
          <td>63</td>
          <td>Retired</td>
          <td>{'email': 'william7285@live.com', 'phone': '1-...</td>
          <td>Colombian</td>
        </tr>
        <tr>
          <th>166622</th>
          <td>499989</td>
          <td>Meri Mathews</td>
          <td>59</td>
          <td>Retired</td>
          <td>{'email': 'thomasena5180@live.com', 'phone': '...</td>
          <td>Greek</td>
        </tr>
        <tr>
          <th>166623</th>
          <td>499995</td>
          <td>Irina Gibbs</td>
          <td>64</td>
          <td>Retired</td>
          <td>{'email': 'delmy_8959@outlook.com', 'phone': '...</td>
          <td>Italian</td>
        </tr>
        <tr>
          <th>166624</th>
          <td>499996</td>
          <td>Emery Anderson</td>
          <td>59</td>
          <td>Retired</td>
          <td>{'email': 'percy6103@live.com', 'phone': '288....</td>
          <td>Spanish</td>
        </tr>
      </tbody>
    </table>
    <p>166625 rows × 6 columns</p>
    </div>

Results
""""""""

.. code:: ipython3

    %%timeit -n1 -r1
    pgreaper.copy_df(retired, name='persons', dbname='pgreaper_test', on_p_key=['occupation'])


.. parsed-literal::

    1 loop, best of 1: 7.8 s per loop
    

Checking Our Work
""""""""""""""""""
    
.. code:: ipython3

    connection_string = pgreaper.PG_DEFAULTS(dbname='pgreaper_test').to_string()

    
.. code:: ipython3

    %%sql $connection_string
    
    SELECT * FROM persons
    WHERE age >= 50 LIMIT 50


.. parsed-literal::

    50 rows affected.
    



.. raw:: html

    <table>
        <tr>
            <th>id</th>
            <th>full_name</th>
            <th>age</th>
            <th>occupation</th>
            <th>contact</th>
            <th>nationality</th>
        </tr>
        <tr>
            <td>915</td>
            <td>Dorsey Shaffer</td>
            <td>53</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;johnny-4695@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;163-554-3792&#x27;}</td>
            <td>Swedish</td>
        </tr>
        <tr>
            <td>1012</td>
            <td>Russel Mccall</td>
            <td>66</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;kenny139@yandex.com&#x27;, &#x27;phone&#x27;: &#x27;1-910-688-4436&#x27;}</td>
            <td>Finnish</td>
        </tr>
        <tr>
            <td>1219</td>
            <td>Guillermo Cooley</td>
            <td>53</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;peter_7368@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;(429) 133-8709&#x27;}</td>
            <td>Cambodian</td>
        </tr>
        <tr>
            <td>1486</td>
            <td>Scott Parker</td>
            <td>63</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;valda-9966@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;1-139-964-0250&#x27;}</td>
            <td>Latvian</td>
        </tr>
        <tr>
            <td>1695</td>
            <td>Merlene King</td>
            <td>61</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;timika-8127@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;901.106.6537&#x27;}</td>
            <td>Estonian</td>
        </tr>
        <tr>
            <td>4239</td>
            <td>Corrinne Frost</td>
            <td>59</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;natisha5688@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;1-804-764-7697&#x27;}</td>
            <td>Saudi</td>
        </tr>
        <tr>
            <td>4717</td>
            <td>Donette Savage</td>
            <td>55</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;page_1478@outlook.com&#x27;, &#x27;phone&#x27;: &#x27;189-685-2682&#x27;}</td>
            <td>Irish</td>
        </tr>
        <tr>
            <td>4769</td>
            <td>Omer Powell</td>
            <td>64</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;tyler_4230@yandex.com&#x27;, &#x27;phone&#x27;: &#x27;189.685.2682&#x27;}</td>
            <td>Uruguayan</td>
        </tr>
        <tr>
            <td>5194</td>
            <td>Regan Joseph</td>
            <td>50</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;azucena_6202@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;1-130-663-8499&#x27;}</td>
            <td>Salvadorian</td>
        </tr>
        <tr>
            <td>5838</td>
            <td>Anton Cannon</td>
            <td>60</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;warren6487@yandex.com&#x27;, &#x27;phone&#x27;: &#x27;429-086-9244&#x27;}</td>
            <td>Irish</td>
        </tr>
        <tr>
            <td>6056</td>
            <td>Gregory Wiley</td>
            <td>50</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;simon1733@live.com&#x27;, &#x27;phone&#x27;: &#x27;1-314-624-3685&#x27;}</td>
            <td>Danish</td>
        </tr>
        <tr>
            <td>6520</td>
            <td>Mason Dodson</td>
            <td>54</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;wiley-8663@live.com&#x27;, &#x27;phone&#x27;: &#x27;1-852-906-6575&#x27;}</td>
            <td>Israeli</td>
        </tr>
        <tr>
            <td>8743</td>
            <td>Emmie Hamilton</td>
            <td>61</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;ali7464@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;(397) 625-4962&#x27;}</td>
            <td>Romanian</td>
        </tr>
        <tr>
            <td>8956</td>
            <td>Leonard Sosa</td>
            <td>62</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;al-4742@outlook.com&#x27;, &#x27;phone&#x27;: &#x27;(179) 891-5062&#x27;}</td>
            <td>Chinese</td>
        </tr>
        <tr>
            <td>13782</td>
            <td>Brent Norris</td>
            <td>54</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;garrett390@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;232.837.7400&#x27;}</td>
            <td>Afghan</td>
        </tr>
        <tr>
            <td>14126</td>
            <td>Son Wilson</td>
            <td>65</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;raye8017@live.com&#x27;, &#x27;phone&#x27;: &#x27;652-370-3678&#x27;}</td>
            <td>Dominican</td>
        </tr>
        <tr>
            <td>14973</td>
            <td>Arcelia Haley</td>
            <td>56</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;marni-2282@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;1-946-957-7639&#x27;}</td>
            <td>Saudi</td>
        </tr>
        <tr>
            <td>15369</td>
            <td>Euna Hahn</td>
            <td>50</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;melita_5569@outlook.com&#x27;, &#x27;phone&#x27;: &#x27;483.145.4282&#x27;}</td>
            <td>English</td>
        </tr>
        <tr>
            <td>15757</td>
            <td>Lory Warner</td>
            <td>66</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;rikki-510@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;(026) 778-3770&#x27;}</td>
            <td>Greek</td>
        </tr>
        <tr>
            <td>16060</td>
            <td>Jessia Blevins</td>
            <td>54</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;phung-7621@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;439.640.8052&#x27;}</td>
            <td>Russian</td>
        </tr>
        <tr>
            <td>17177</td>
            <td>Von Mullen</td>
            <td>63</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;ty-7154@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;910-148-9413&#x27;}</td>
            <td>Ukrainian</td>
        </tr>
        <tr>
            <td>17336</td>
            <td>Serita Gregory</td>
            <td>62</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;ilda7522@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;039-630-0154&#x27;}</td>
            <td>Egyptian</td>
        </tr>
        <tr>
            <td>19998</td>
            <td>Arnold Mcmillan</td>
            <td>60</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;dong6181@live.com&#x27;, &#x27;phone&#x27;: &#x27;149.377.4981&#x27;}</td>
            <td>Danish</td>
        </tr>
        <tr>
            <td>20482</td>
            <td>Jeneva Crosby</td>
            <td>53</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;lizeth-3856@live.com&#x27;, &#x27;phone&#x27;: &#x27;545.029.6990&#x27;}</td>
            <td>Dutch</td>
        </tr>
        <tr>
            <td>22282</td>
            <td>Klara Sutton</td>
            <td>54</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;nicholle9797@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;884-671-8260&#x27;}</td>
            <td>Dutch</td>
        </tr>
        <tr>
            <td>22334</td>
            <td>Noe Fox</td>
            <td>51</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;angelo-5064@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;271.013.9775&#x27;}</td>
            <td>Irish</td>
        </tr>
        <tr>
            <td>22431</td>
            <td>Maye Newman</td>
            <td>52</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;jeremy-5043@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;1-579-666-7433&#x27;}</td>
            <td>Venezuelan</td>
        </tr>
        <tr>
            <td>22694</td>
            <td>Chante Adams</td>
            <td>51</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;kizzie_7226@live.com&#x27;, &#x27;phone&#x27;: &#x27;(887) 174-3442&#x27;}</td>
            <td>French</td>
        </tr>
        <tr>
            <td>22849</td>
            <td>Ron Nieves</td>
            <td>51</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;teodoro7237@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;823-299-5268&#x27;}</td>
            <td>Argentinian</td>
        </tr>
        <tr>
            <td>23326</td>
            <td>Louis Watkins</td>
            <td>57</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;marlin_6498@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;1-582-372-1588&#x27;}</td>
            <td>Chilean</td>
        </tr>
        <tr>
            <td>23752</td>
            <td>Lowell Burton</td>
            <td>57</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;john4744@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;969.176.7855&#x27;}</td>
            <td>Danish</td>
        </tr>
        <tr>
            <td>24021</td>
            <td>Dotty English</td>
            <td>56</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;waneta714@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;519.705.0652&#x27;}</td>
            <td>Saudi</td>
        </tr>
        <tr>
            <td>25127</td>
            <td>Rocky Scott</td>
            <td>60</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;wendell5688@outlook.com&#x27;, &#x27;phone&#x27;: &#x27;681.405.1298&#x27;}</td>
            <td>Swedish</td>
        </tr>
        <tr>
            <td>25817</td>
            <td>Juan Chapman</td>
            <td>60</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;basil3123@live.com&#x27;, &#x27;phone&#x27;: &#x27;1-601-774-1365&#x27;}</td>
            <td>Japanese</td>
        </tr>
        <tr>
            <td>26060</td>
            <td>Earleen Monroe</td>
            <td>51</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;raelene_2099@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;390.346.9362&#x27;}</td>
            <td>Irish</td>
        </tr>
        <tr>
            <td>26131</td>
            <td>Kurtis Bates</td>
            <td>55</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;leigh-5232@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;(847) 681-7496&#x27;}</td>
            <td>Dutch</td>
        </tr>
        <tr>
            <td>26411</td>
            <td>Stephane Witt</td>
            <td>53</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;beata4873@outlook.com&#x27;, &#x27;phone&#x27;: &#x27;(713) 298-5607&#x27;}</td>
            <td>Irish</td>
        </tr>
        <tr>
            <td>26769</td>
            <td>Ja Wilkinson</td>
            <td>60</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;sammy6558@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;467.269.2270&#x27;}</td>
            <td>Finnish</td>
        </tr>
        <tr>
            <td>27041</td>
            <td>Cordia Eaton</td>
            <td>60</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;clarine2380@outlook.com&#x27;, &#x27;phone&#x27;: &#x27;1-550-095-1353&#x27;}</td>
            <td>Ukrainian</td>
        </tr>
        <tr>
            <td>27678</td>
            <td>Ricky Jacobs</td>
            <td>63</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;robin-5899@live.com&#x27;, &#x27;phone&#x27;: &#x27;560-128-1809&#x27;}</td>
            <td>Mexican</td>
        </tr>
        <tr>
            <td>27830</td>
            <td>Phung Melendez</td>
            <td>59</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;ka-105@live.com&#x27;, &#x27;phone&#x27;: &#x27;948-647-5247&#x27;}</td>
            <td>Salvadorian</td>
        </tr>
        <tr>
            <td>28624</td>
            <td>Rebecka Witt</td>
            <td>64</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;hassie_5350@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;539.580.3269&#x27;}</td>
            <td>Finnish</td>
        </tr>
        <tr>
            <td>29365</td>
            <td>Tod Williamson</td>
            <td>55</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;dong4168@live.com&#x27;, &#x27;phone&#x27;: &#x27;1-497-083-3206&#x27;}</td>
            <td>German</td>
        </tr>
        <tr>
            <td>33514</td>
            <td>Jess Rowland</td>
            <td>64</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;nigel-433@outlook.com&#x27;, &#x27;phone&#x27;: &#x27;588.713.9422&#x27;}</td>
            <td>Polish</td>
        </tr>
        <tr>
            <td>34040</td>
            <td>Leigh Schneider</td>
            <td>61</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;carey2562@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;1-888-226-2833&#x27;}</td>
            <td>Latvian</td>
        </tr>
        <tr>
            <td>34190</td>
            <td>So Morales</td>
            <td>66</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;lahoma-6033@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;(636) 975-3145&#x27;}</td>
            <td>Italian</td>
        </tr>
        <tr>
            <td>35249</td>
            <td>Alvaro Franks</td>
            <td>63</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;cecil-5400@outlook.com&#x27;, &#x27;phone&#x27;: &#x27;567.210.8698&#x27;}</td>
            <td>German</td>
        </tr>
        <tr>
            <td>36898</td>
            <td>Minna Glass</td>
            <td>66</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;caterina-9301@gmail.com&#x27;, &#x27;phone&#x27;: &#x27;+1-(992)-032-8425&#x27;}</td>
            <td>Estonian</td>
        </tr>
        <tr>
            <td>37215</td>
            <td>Jacelyn Knight</td>
            <td>59</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;berneice2272@yahoo.com&#x27;, &#x27;phone&#x27;: &#x27;(886) 773-4378&#x27;}</td>
            <td>English</td>
        </tr>
        <tr>
            <td>37302</td>
            <td>Stanton Leblanc</td>
            <td>52</td>
            <td>Retired</td>
            <td>{&#x27;email&#x27;: &#x27;elwood-4643@live.com&#x27;, &#x27;phone&#x27;: &#x27;1-886-773-4378&#x27;}</td>
            <td>Irish</td>
        </tr>
    </table>



Where's the Bottleneck
----------------------

Apparently it only takes Python about 2.5 seconds to create the 160,000
row UPSERT statement (which includes properly encoding dicts, escaping quotes, and so on). Since `psycopg2` (which PGReaper sends the UPSERT statement to)
is basically a C library with Python bindings, and we're only sending one 
statement, the 5 remaining seconds is most likely taken up primarily by Postgres itself.

.. code:: ipython3

    %%timeit
    pgreaper.postgres.loader._unnest(
        pgreaper.pandas_to_table(retired))


.. parsed-literal::

    1 loop, best of 3: 2.42 s per loop
    
