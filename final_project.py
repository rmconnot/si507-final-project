from datapackage import Package
import plotly.graph_objs as go
import csv
import json
import requests
import sqlite3

########################
###  Robert Connot #####
###  rmconnot  #########
########################

package = Package('https://datahub.io/rufuspollock/oscars-nominees-and-winners/datapackage.json')
CACHE_FILENAME = 'final_project_cache.json'
CACHE_DICT = {}

conn = sqlite3.connect("awards_movies.sqlite")
cur = conn.cursor()



def save_cache(cache_dict):
    ''' Saves the current state of the cache to disk
    
    Parameters
    ----------
    cache_dict: dict
        The dictionary to save
    
    Returns
    -------
    None
    '''

    dumped_json_cache = json.dumps(cache_dict)
    fw = open(CACHE_FILENAME,"a")
    fw.write(dumped_json_cache)
    fw.close()


def open_cache():
    ''' Opens the cache file if it exists and loads the JSON into
    the CACHE_DICT dictionary.
    if the cache file doesn't exist, creates a new cache dictionary
    
    Parameters
    ----------
    None
    
    Returns
    -------
    The opened cache: dict
    '''

    try:
        cache_file = open(CACHE_FILENAME, 'r')
        cache_contents = cache_file.read()
        cache_dict = json.loads(cache_contents)
        cache_file.close()
    except:
        cache_dict = {}
    
    return cache_dict


def retrieve_awards_data():
    """
    Pulls data from the Academy Awards dataset

    Parameters
    -----------
    None

    Returns
    -----------
    awards_data: dict
    contains the academy awards dataset
    """

    awards_data = {'awards_data' : []}
    for resource in package.resources:
        if resource.descriptor['datahub']['type'] == 'derived/csv':
            data = resource.read()
    for award in data:
        awards_data['awards_data'].append(award)
    CACHE_DICT.update(awards_data)
    return awards_data



def retrieve_movies_data(awards_data):
    """
    Uses the academy awards data to make requests to the OMDb API

    Parameters
    ------------
    awards_data: dict
    dictionary containing data pulled from the academy awards dataset

    Returns
    --------
    None
    """

    params = {'t' : '', 'type' : 'movie'}
    movies_data = {'movies_data' : []}
    for award in awards_data['awards_data']:
        if int(award[0]) > 1940 and 'picture' in award[1].lower():
            params['t'] = award[3].lower()
            response = requests.get('http://www.omdbapi.com/?apikey=a497baba&', params = params)
            result = response.json()
            if result not in movies_data['movies_data']:
                movies_data['movies_data'].append(result)
    CACHE_DICT.update(movies_data)
    save_cache(CACHE_DICT)
    

def retrieve_data_with_cache():
    """
    Determines whether a cache exists: if it does, then the function returns the cache - if it doesn't, 
    then the function retrieves the data and places it into the cache dictionary

    Parameters
    ------------
    None

    Returns
    --------
    CACHE_DICT: dict
    Contains all of the necessary information to populate the database
    """

    if CACHE_DICT:
        print('Using cache...')
        return CACHE_DICT
    else:
        retrieve_movies_data(retrieve_awards_data())
        return CACHE_DICT



def create_database():
    """
    Adds tables to the database

    Parameters
    ------------
    None

    Returns
    --------
    None
    """

    drop_movies = '''
        DROP TABLE IF EXISTS "Movies"
    '''

    drop_awards = '''
        DROP TABLE IF EXISTS "Awards"
    '''

    create_movies = '''
        CREATE TABLE IF NOT EXISTS "Movies" (
            "Id"                    INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            "Title"                 TEXT NOT NULL,
            "MovieYear"             INTEGER NOT NULL,
            "Runtime"               INTEGER NOT NULL,
            "IMDbRating"            REAL,
            "RottenTomatoesRating"  INTEGER,
            "Metascore"             INTEGER
        );
    '''

    create_awards = '''
            CREATE TABLE IF NOT EXISTS "Awards" (
            "Id"                    INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            "Year"                  INTEGER NOT NULL,
            "Category"              TEXT NOT NULL,
            "Winner"                BIT NOT NULL,
            "Entity"                TEXT NOT NULL,
            "MovieID"               INTEGER
        );
    '''

    cur.execute(drop_movies)
    cur.execute(drop_awards)

    cur.execute(create_movies)
    cur.execute(create_awards)

    conn.commit()



def populate_awards_data():
    """
    Adds data to the awards table in the database

    Parameters
    ------------
    None

    Returns
    --------
    None
    """

    add_command = '''
    INSERT INTO Awards
    VALUES (?, ?, ?, ?, ?, ?)
    '''

    command_values = []
    for award in CACHE_DICT['awards_data']:
        if type(award[0]) == int:
            year = award[0]
        else:
            year = award[0][0]
        category = award[1]
        winner = award[2]
        entity = award[3].replace('"', '')
        

        search_id_command = f'SELECT Id FROM Movies WHERE Title = "{entity}"'
        cur.execute(search_id_command)
        data = cur.fetchall()
        if data:
            movie_id = data[0][0]
        else:
            movie_id = None


        command_values = [None, year, category, winner, entity, movie_id]


        cur.execute(add_command, command_values)
        conn.commit()

    


def populate_movies_data():
    """
    Adds data to the movies table in the database

    Parameters
    ------------
    None

    Returns
    --------
    None
    """

    add_command = '''
    INSERT INTO Movies
    VALUES (?, ?, ?, ?, ?, ?, ?)
    '''
    rotten_tomatoes = ''
    command_values = []
    for movie in CACHE_DICT['movies_data'][1:]:
        title = movie['Title']
        year = int(movie['Year'])
        if len(movie['Runtime']) > 3:
            runtime = int(movie['Runtime'][0:-4])
        try:
            imdb_rating = float(movie['imdbRating'])
        except:
            imdb_rating = None
        if movie['Metascore'].isnumeric() == True:
            metascore = int(movie['Metascore'])
        else:
            metascore = None
        for source_value in movie['Ratings']:
            if source_value['Source'] == 'Rotten Tomatoes':
                rotten_tomatoes = int(source_value['Value'][0:-1])
        
        command_values = [None, title, year, runtime, imdb_rating, rotten_tomatoes, metascore]
        
        cur.execute(add_command, command_values)
        conn.commit()


def get_award_categories(year):
    """
    Makes a SQL statement that returns all of the award categories that exist for the given year which have films as nominees

    Parameters
    ------------
    year: int
    the year that the user inputted

    Returns
    --------
    category_dict: dict
    contains all of the categories for the year as values and auto-incrementing numbers as keys
    """

    i = 1
    category_dict = {}
    categories_command = f'SELECT DISTINCT Category FROM Awards WHERE MovieID != "NULL" AND Year = "{year}"'
    cur.execute(categories_command)
    data = cur.fetchall()
    
    for item in data:
        category_dict[i] = item[0]
        i += 1
    
    return(category_dict)


def display_results(xvals, yvals, year, category, criteria):
    """
    Uses plotly to display the results of the user input

    Parameters
    ------------
    xvals: list
    yvals: list
    year: int
    category: str
    criteria str

    Returns
    --------
    None
    """

    bar_data = go.Bar(x=xvals, y=yvals)
    basic_layout = go.Layout(title=f"{year} {category} Nominees by {criteria}")
    fig = go.Figure(data=bar_data, layout=basic_layout)
    fig.show()


def process_ranking_critera(year, category, criteria_number):
    """
    Processes the last user input by determining which criteria should be used to rank the nominees. Calls the display_results() function
    to display the results

    Parameters
    ------------
    year: int
    category: str
    criteria_number: int
    supplied by the user through input

    Returns
    --------
    None
    """

    if criteria_number == 1:
        cur.execute(f'SELECT DISTINCT Runtime, Title FROM Movies JOIN Awards ON Awards.MovieID = Movies.Id WHERE MovieYear = "{year}" AND Category = "{category}"')
        data = cur.fetchall()
        xvals = []
        yvals = []

        for item in data:
            yvals.append(item[0])
            xvals.append(item[1])
        
        display_results(xvals, yvals, year, category, 'Runtime')

    if criteria_number == 2:
        cur.execute(f'SELECT DISTINCT IMDbRating, Title FROM Movies JOIN Awards ON Awards.MovieID = Movies.Id WHERE MovieYear = "{year}" AND Category = "{category}"')
        data = cur.fetchall()
        xvals = []
        yvals = []

        for item in data:
            yvals.append(item[0])
            xvals.append(item[1])
        
        display_results(xvals, yvals, year, category, 'IMDb Rating')

    if criteria_number == 3:
        cur.execute(f'SELECT DISTINCT RottenTomatoesRating, Title FROM Movies JOIN Awards ON Awards.MovieID = Movies.Id WHERE MovieYear = "{year}" AND Category = "{category}"')
        data = cur.fetchall()
        xvals = []
        yvals = []

        for item in data:
            yvals.append(item[0])
            xvals.append(item[1])
        
        display_results(xvals, yvals, year, category, 'Rotten Tomatoes Score')

    if criteria_number == 4:
        cur.execute(f'SELECT Metascore, Title FROM Movies JOIN Awards ON Awards.MovieID = Movies.Id WHERE MovieYear = "{year}" AND Category = "{category}"')
        data = cur.fetchall()
        xvals = []
        yvals = []

        for item in data:
            yvals.append(item[0])
            xvals.append(item[1])
        
        display_results(xvals, yvals, year, category, 'Metascore')


def interactive_prompt():
    """
    Presents the user with an interactive prompt for inputting commands. Also determines which function to pass the command to

    Parameters
    -----------
    None

    Returns
    ---------
    None
    """
    
    i = 0
    response = ''
    while response != 'exit' and i == 0:
        response = input('\nEnter a year (must be between 1927 - 2017): ')
        print('\n\n')
        if response.isnumeric() == True:
            if int(response) >= 1927 and int(response) <= 2017:
                year = int(response)
                award_categories_dict = get_award_categories(response)
                for key, value in award_categories_dict.items():
                    print(f"{key}. {value}")
                print('------------------------------------\n')
                i = 1
            
                while response != 'exit' and i == 1:
                    response = input('Select a Nomination Category: ')
                    print('\n\n')
                    if response.isnumeric() == True:
                        if int(response) in award_categories_dict.keys():
                            category = award_categories_dict[int(response)]
                            i = 2

                        else:
                            print(f'ERROR - please enter a number from the list above') 

                        while response != 'exit' and i == 2:
                            print("1. Runtime\n2. IMDb Rating\n3. Rotten Tomatoes Score\n4. Metascore\n-------------------------------\n")
                            response = input('Choose a criterion for ranking the nominees: ')
                            if response.isnumeric() == True:
                                if int(response) <= 4 and int(response) >= 1:
                                    criteria_number = int(response)
                                    process_ranking_critera(year, category, criteria_number)
                                    print('\n')
                                    i = 0

                                
                        
                                
                                else:
                                    print(f'ERROR - please enter a number from the list above')
                            else:
                                print(f'ERROR - please enter a number from the list above')                                           
                    else:
                        print(f'ERROR - please enter a number from the list above')
            else:
                print(f'ERROR - please enter a valid year')
        else:
            print(f'ERROR - please enter a valid year')




if __name__=="__main__":
    CACHE_DICT = open_cache()
    print('\nLoading...')
    
    CACHE_DICT = retrieve_data_with_cache()
    print('Loading...')
    
    create_database()
    print('Loading...')
    
    populate_movies_data()
    print('Loading...')
    
    populate_awards_data()
    print('Loading...')


    interactive_prompt()
