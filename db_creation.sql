drop database if exists group_project;

create database if not exists group_project;

use group_project;

create table if not exists city_attributes
(
    City varchar (100),
    Country varchar (100),
    Latitude float,
    Longitude float
);

create table if not exists humidity
(
    datetime datetime,
    city varchar (100),
    humidity float
);

create table if not exists pressure
(
    datetime datetime,
    city varchar (100),
    pressure float
);

create table if not exists temperature
(
    datetime datetime,
    city varchar (100),
    temperature float
);

create table if not exists weather_description
(
    datetime datetime,
    city varchar (100),
    weather_description varchar(100)
);


create table if not exists wind_direction
(
    datetime datetime,
    city varchar (100),
    wind_direction float
);

create table if not exists wind_speed
(
    datetime datetime,
    city varchar (100),
    wind_speed float
);


