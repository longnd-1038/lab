import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key

def createMovieTable():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.create_table(
        TableName='Movie',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH' # Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  # Partition key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def putMovie(title, year, plot, rating, dynamodb=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Movie')
    response = table.put_item(
        Item={
            'year': year,
            'title': title,
            'info': {
                'plot': plot,
                'rating': rating
            }
        }
    )

    return response


def getMovie(title, year, dynamodb=None):
    dynamoDB = boto3.resource('dynamodb')
    table = dynamoDB.Table('Movie')
    response = table.get_item(Key={'year': year, 'title': title})

    return response

def updateMovie(title, year, rating, plot, dynamodb=None):
    dynamoDB = boto3.resource('dynamodb')
    table = dynamoDB.Table('Movie')
    response = table.update_item(
        Key={
            'year': year,
            'title': title
        },
        UpdateExpression="set info.rating=:r, info.plot=:p",
        ExpressionAttributeValues={
            ':r': Decimal(rating),
            ':p': plot,
        },
        ReturnValues="UPDATED_NEW"
    )

    return response


def deleteMovie(title, year, dynamodb=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Movie')
    response = table.delete_item(
        Key={
            'year': year,
            'title': title
        }
    )

    return response

def queryMovies(year):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Movie')
    response = table.query(
        KeyConditionExpression=Key('year').eq(year)
    )

    return response['Items']

def deleteTableData():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Movie')
    table.delete()

def scan_movies(year_range, dynamodb=None):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('Movie')
    scan_kwargs = {
        'FilterExpression': Key('year').between(*year_range),
        'ProjectionExpression': "#yr, title, info.rating",
        'ExpressionAttributeNames': {"#yr": "year"}
    }

    response = table.scan(**scan_kwargs)
    return response['Items']