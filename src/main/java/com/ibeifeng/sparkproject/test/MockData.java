package com.ibeifeng.sparkproject.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.spark.session.CategorySortKey;
import com.ibeifeng.sparkproject.util.SparkUtils;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.StringUtils;

import static com.ibeifeng.sparkproject.util.SparkUtils.getSQLContext;

/**
 * 模拟数据程序
 * @author Administrator
 *
 */
public class MockData {

	/**
	 * 模拟数据
	 * @param sc
	 * @param sqlContext
	 */
	public static void mock(JavaSparkContext sc,
			SQLContext sqlContext) {

		List<Row> rows = new ArrayList<Row>();
		String[] searchKeywords = new String[] {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
				"呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
		String date = DateUtils.getTodayDate();
		String[] actions = new String[]{"search", "click", "order", "pay"};
		Random random = new Random();
		
		for(int i = 0; i < 100; i++) {
			long userid = random.nextInt(100);    
			
			for(int j = 0; j < 10; j++) {
				String sessionid = UUID.randomUUID().toString().replace("-", "");  
				String baseActionTime = date + " " + random.nextInt(23);
				
				Long clickCategoryId = null;
				  
				for(int k = 0; k < random.nextInt(100); k++) {
					long pageid = random.nextInt(10);    
					String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
					String searchKeyword = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;
					
					String action = actions[random.nextInt(4)];
					if("search".equals(action)) {
						searchKeyword = searchKeywords[random.nextInt(10)];   
					} else if("click".equals(action)) {
						if(clickCategoryId == null) {
							clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));    
						}
						clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));  
					} else if("order".equals(action)) {
						orderCategoryIds = String.valueOf(random.nextInt(100));  
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if("pay".equals(action)) {
						payCategoryIds = String.valueOf(random.nextInt(100));  
						payProductIds = String.valueOf(random.nextInt(100));
					}
					
					Row row = RowFactory.create(date, userid, sessionid, 
							pageid, actionTime, searchKeyword,
							clickCategoryId, clickProductId,
							orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds, 
							Long.valueOf(String.valueOf(random.nextInt(10))));    
					rows.add(row);
				}
			}
		}
		
		JavaRDD<Row> rowsRDD = sc.parallelize(rows);
		
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("city_id", DataTypes.LongType, true)));
		
		DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);
		
		df.registerTempTable("user_visit_action");  
		for(Row _row : df.take(1)) {
			System.out.println(_row);  
		}
		
		/**
		 * ==================================================================
		 */
		
		rows.clear();
		String[] sexes = new String[]{"male", "female"};
		for(int i = 0; i < 100; i ++) {
			long userid = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = random.nextInt(60);
			String professional = "professional" + random.nextInt(100);
			String city = "city" + random.nextInt(100);
			String sex = sexes[random.nextInt(2)];
			
			Row row = RowFactory.create(userid, username, name, age, 
					professional, city, sex);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("professional", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true)));
		
		DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
		for(Row _row : df2.take(1)) {
			System.out.println(_row);  
		}
		
		df2.registerTempTable("user_info");  
		
		/**
		 * ==================================================================
		 */
		rows.clear();
		
		int[] productStatus = new int[]{0, 1};
		
		for(int i = 0; i < 100; i ++) {
			long productId = i;
			String productName = "product" + i;
			String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";    
			
			Row row = RowFactory.create(productId, productName, extendInfo);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema3 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("product_id", DataTypes.LongType, true),
				DataTypes.createStructField("product_name", DataTypes.StringType, true),
				DataTypes.createStructField("extend_info", DataTypes.StringType, true)));
		
		DataFrame df3 = sqlContext.createDataFrame(rowsRDD, schema3);
		for(Row _row : df3.take(1)) {
			System.out.println(_row);  
		}
		
		df3.registerTempTable("product_info"); 
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
//				.set("spark.default.parallelism", "100")
				.set("spark.storage.memoryFraction", "0.5")
				.set("spark.shuffle.consolidateFiles", "true")
				.set("spark.shuffle.file.buffer", "64")
				.set("spark.shuffle.memoryFraction", "0.3")
				.set("spark.reducer.maxSizeInFlight", "24")
				.set("spark.shuffle.io.maxRetries", "60")
				.set("spark.shuffle.io.retryWait", "60")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		SparkUtils.setMaster(conf);

		/**
		 * 比如，获取top10热门品类功能中，二次排序，自定义了一个Key
		 * 那个key是需要在进行shuffle的时候，进行网络传输的，因此也是要求实现序列化的
		 * 启用Kryo机制以后，就会用Kryo去序列化和反序列化CategorySortKey
		 * 所以这里要求，为了获取最佳性能，注册一下我们自定义的类
		 */

		JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.checkpointFile("hdfs://");
		SQLContext sqlContext = getSQLContext(sc.sc());
		new MockData().mock(sc,sqlContext);
	}
	
}
