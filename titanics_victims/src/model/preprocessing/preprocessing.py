"""module for data preparation"""
import pandas as pd
from sklearn import preprocessing


def simplify_ages(df):
    """Group people by ages"""
    df.Age = df.Age.fillna(-0.5)
    bins = (-1, 0, 5, 12, 18, 25, 35, 60, 120)
    group_names = ['Unknown', 'Baby', 'Child', 'Teenager', 'Student', 'Young Adult', 'Adult', 'Senior']
    categories = pd.cut(df.Age, bins, labels=group_names)
    df.Age = categories
    return df


def simplify_cabins(df):
    """Simplify cabin feature"""
    df.Cabin = df.Cabin.fillna('N')
    df.Cabin = df.Cabin.apply(lambda x: x[0])
    return df


def simplify_fares(df):
    """Handle fare feature"""
    df.Fare = df.Fare.fillna(-0.5)
    bins = (-1, 0, 8, 15, 31, 1000)
    group_names = ['Unknown', '1_quartile', '2_quartile', '3_quartile', '4_quartile']
    categories = pd.cut(df.Fare, bins, labels=group_names)
    df.Fare = categories
    return df


def format_name(df):
    """Handle names"""
    df['Lname'] = df.Name.apply(lambda x: x.split(' ')[0])
    df['NamePrefix'] = df.Name.apply(lambda x: x.split(' ')[1])
    return df


def drop_features(df):
    """Drop useless features"""
    return df.drop(['Ticket', 'Name', 'Embarked'], axis=1)


def transform_features(df):
    """Feature preprocessing pipeline"""
    df = simplify_ages(df)
    df = simplify_cabins(df)
    df = simplify_fares(df)
    df = format_name(df)
    df = drop_features(df)
    return df


def encode_features(dfs: list, encoders: dict = None):
    """Encode categorical features"""
    features = ['Fare', 'Cabin', 'Age', 'Sex', 'Lname', 'NamePrefix']
    df_combined = pd.concat([df[features] for df in dfs])

    for feature in features:
        le = encoders.get(feature)
        if not le:
            le = preprocessing.LabelEncoder()
            le = le.fit(df_combined[feature])
        for df in dfs:
            df[feature] = le.transform(df[feature])
    return dfs
