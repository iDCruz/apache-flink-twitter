package com.davidcruz.apacheflinktwitter.tutorials.movieratings

import groovy.transform.ToString
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.util.Collector

import java.util.function.Function
import java.util.stream.Collectors
//tutorial/course - https://www.pluralsight.com/courses/understanding-apache-flink
class FilterMovies {
    static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment()

    static void main(String[] args) {
//        WriteDramaMovies()
        GetAverageRatingPerGenre()
    }

    static void WriteDramaMovies() {
        DataSet<Tuple3<Long, String, String>> ds = env.readCsvFile("ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"' as char)
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class)

        ds.map(new MapFunction<Tuple3<Long, String, String>, Movie>() {
            @Override
            Movie map(Tuple3<Long, String, String> line) throws Exception {
                Set<String> genres = new HashSet<>(Arrays.asList(line.f2.split("\\|")))
                return new Movie(id: line.f0, name: line.f1, genres: genres)
            }
        }).filter(new FilterFunction<Movie>() {
            @Override
            boolean filter(Movie movie) throws Exception {
                return movie.genres.contains("Drama")
            }
        })
                .writeAsText("drama-movies-output")
        env.execute()
    }

    //todo replace immutable types with value types to optimize
    static void GetAverageRatingPerGenre() {
        DataSet<Tuple3<Long, String, String>> ds = env.readCsvFile("ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"' as char)
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class)

        DataSet<Movie> movies = ds.map(new MapFunction<Tuple3<Long, String, String>, Movie>() {
            @Override
            Movie map(Tuple3<Long, String, String> line) throws Exception {
                Set<String> genres = new HashSet<>(Arrays.asList(line.f2.split("\\|")))
                return new Movie(id: line.f0, name: line.f1, genres: genres)
            }
        })

        DataSet<Tuple2<Long, Double>> ds2 = env.readCsvFile("ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .ignoreInvalidLines()
                .includeFields(false, true, true, false)
                .types(Long.class, Double.class)

        DataSet<Rating> ratings = ds2.map(new MapFunction<Tuple2<Long, Double>, Rating>() {
            @Override
            Rating map(Tuple2<Long, Double> line) throws Exception {
                return new Rating(movieId: line.f0, rating: line.f1)
            }
        })

        List<Tuple2<String, Double>> averages = movies.join(ratings)
            .where("id")
            .equalTo("movieId")
            .with(new JoinFunction<Movie, Rating, Tuple3<String, String, Double>>() {
                @Override
                Tuple3<String, String, Double> join(Movie movie, Rating rating) throws Exception {
                    return new Tuple3<String, String, Double>(movie.name, movie.genres[0], rating.rating)
                }
            })
            .groupBy(1)
            .reduceGroup(new GroupReduceFunction<Tuple3<String, String, Double>, Tuple2<String, Double>>() {
                @Override
                void reduce(Iterable<Tuple3<String, String, Double>> values, Collector<Tuple2<String, Double>> collector) throws Exception {
                    String genre = null
                    int count = 0
                    double sum = 0
                    for(Tuple3<String, String, Double> value:values){
                        genre = value.f1
                        sum+= value.f2
                        count++
                    }

                    collector.collect(new Tuple2<String, Double>(genre, sum/count))
                }
            })
            .collect()

        String result = averages.stream()
            .sorted(new Comparator<Tuple2<String, Double>>() {
                @Override
                int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                    Double.compare(o1.f1, o2.f1)
                }
            })
            .map(new Function<Tuple2<String, Double>, String>() {
                @Override
                String apply(Tuple2<String, Double> tuple2) {
                    return "${tuple2.f0}: ${tuple2.f1}"
                }
            })
            .collect(Collectors.joining("\n"))

        print(result)
    }
}

@ToString
class Rating {
    Long movieId
    Double rating
}

@ToString
class Movie {
    Long id
    String name
    Set<String> genres
}