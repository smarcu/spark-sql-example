package com.intelliware.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSqlExample {
	
	
	public static void main(String s[]) throws Exception {
		
		SparkConf conf = new SparkConf().setAppName("SparkSQLExample");
		// to run in eclipse, uncomment the following line
		conf.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new SQLContext( sc );
		
		DataFrame df = sqlContext.read().json("src/main/resources/books.json");
		// needed to do sql 
		df.registerTempTable("books");
		
		df.show();
		df.printSchema();
		
		// show all books for Tolkien
		df.filter(df.col("author").eqNullSafe("J.R.R.Tolkien")).show();
		
		// use sql
		df.sqlContext().sql("select title from books").show();
		df.sqlContext().sql("select author, title from books where author = 'J.R.R.Tolkien'").show();

		// reviews

		DataFrame dfRevs = sqlContext.read().json("src/main/resources/book-reviews.json");
		// needed to do sql 
		dfRevs.registerTempTable("book_reviews");
		
		dfRevs.show();
		dfRevs.printSchema();
		
		// show all reviews for Tolkien
		dfRevs.filter(dfRevs.col("author").eqNullSafe("J.R.R.Tolkien")).show();
		
		// use sql
		dfRevs.sqlContext().sql("select title from book_reviews").show();
		dfRevs.sqlContext().sql("select author, title from book_reviews where author = 'J.R.R.Tolkien'").show();

		// join the two sources and calculate average ratings per book
		df.join(dfRevs, df.col("author").equalTo(dfRevs.col("author"))).groupBy(dfRevs.col("author"), dfRevs.col("title")).avg("stars").show();
		
		
		
	}

}
