val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//Read CSV data files
val dhol_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("Dataset_Holidays.txt")
val dtrpt_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("Dataset_Transport.txt")
val dusr_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("Dataset_User_details.txt")

//Join of Holidays, Transport, User_Details
val dholusr_df = dhol_df.join(dusr_df, dhol_df.col("userid") === dusr_df.col("userid"))
val dholusrtrpt_df = dholusr_df.join(dtrpt_df, dholusr_df.col("travelmode") === dtrpt_df.col("travelmode"))

//Anomynous function which takes 2 Integer and returns the product
val mulColumn : (Int,Int)=>Int=(num1:Int,num2:Int)=>{num1*num2}
    
//Declare the UDF
val mulColumnUDF = udf(mulColumn)
    
//Modified Dataframe with Amount, add the new column “amount” by calling the udf
val dholusrtrptamt_df = dholusrtrpt_df.withColumn("amount",mulColumnUDF(dholusrtrpt_df.col("distance"),dholusrtrpt_df.col("costperunit")))

//Which route is generating the most revenue per year
dholusrtrptamt_df.groupBy("src","dest").sum("amount").sort(desc("sum(amount)")).show()
dholusrtrptamt_df.groupBy("dest","src").sum("amount").sort(desc("sum(amount)")).show()

//What is the total amount spent by every user on air-travel per year    
dholusrtrptamt_df.groupBy("yearoftravel","name").sum("amount").sort(desc("yearoftravel"),desc("sum(amount)")).show()

//Anomynous function which determines age group
val chkAge : (Int)=>Int=(num1:Int)=>{if (num1 < 20) 1 else if (num1 < 35) 2 else 3}

//Declare the UDF
val chkAgeUDF = udf(chkAge)

//Modified Dataframe with Age Group, add the new column “agegrp” by calling the udf
val dholusrtrptagrp_df = dholusrtrpt_df.withColumn("agegrp",chkAgeUDF(dholusrtrpt_df.col("age")))

//Considering age groups of < 20 , 20-35, 35 > , which age group is travelling the most every year
dholusrtrptagrp_df.groupBy("yearoftravel","agegrp").sum("distance").sort(desc("yearoftravel"),desc("sum(distance)")).show()
