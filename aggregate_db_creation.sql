drop database if exists group_project_aggregates;

create database if not exists group_project_aggregates;

create schema if not exists group_project_aggregates.weather_data;

use group_project_aggregates.weather_data;

create table if not exists cities
(
    City varchar (100),
    Country varchar (100),
    Latitude float,
    Longitude float
);

create table if not exists humidity_daily_average
(
    date date,
    city varchar (100),
    humidity float,
    min_humidity float,
    max_humidity float
);


create table if not exists wind_speed_daily_average
(
    date date,
    city varchar (100),
    wind_speed float,
    min_wind_speed float, 
    max_wind_speed float
);

create table if not exists daily_temperature
(
    date date,
    city varchar (100),
    temperature float,
    min_temperature float,
    max_temperature float
);



create table if not exists daily_pressure
(
    date date,
    city varchar (100),
    pressure float,
    min_pressure float,
    max_pressure float
);


create table if not exists wind_direction_daily_average
(
    date date,
    city varchar (100),
    wind_direction varchar(50)
);



ALTER TABLE group_project_aggregates.cities ADD COLUMN `id` int(10) UNSIGNED PRIMARY KEY AUTO_INCREMENT FIRST;

ALTER TABLE group_project_aggregates.humidity_daily_average ADD COLUMN `id` int(10) UNSIGNED PRIMARY KEY AUTO_INCREMENT FIRST;
ALTER TABLE group_project_aggregates.wind_speed_daily_average ADD COLUMN `id` int(10) UNSIGNED PRIMARY KEY AUTO_INCREMENT FIRST;
ALTER TABLE group_project_aggregates.daily_temperature ADD COLUMN `id` int(10) UNSIGNED PRIMARY KEY AUTO_INCREMENT FIRST;
ALTER TABLE group_project_aggregates.daily_pressure ADD COLUMN `id` int(10) UNSIGNED PRIMARY KEY AUTO_INCREMENT FIRST;
ALTER TABLE group_project_aggregates.wind_direction_daily_average ADD COLUMN `id` int(10) UNSIGNED PRIMARY KEY AUTO_INCREMENT FIRST;






