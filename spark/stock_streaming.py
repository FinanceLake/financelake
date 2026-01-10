from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import isnan
from delta import *
import time, random, builtins


class DeltaLakeStockAnalyzer:
    """
    FinanceLake
    Bronze → Silver → Gold → ML → Reinforcement Learning
    """

    def __init__(self, kafka_bootstrap_servers="kafka:9092"):
        builder = SparkSession.builder \
            .appName("FinanceLake_Delta_ML_RL_REAL") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        self.kafka_bootstrap_servers = kafka_bootstrap_servers

        # Paths
        self.bronze_path = "/delta/bronze"
        self.silver_path = "/delta/silver"
        self.gold_path = "/delta/gold"
        self.rl_path = "/delta/gold_rl"

        # RL parameters
        self.actions = [-1, 0, 1]   # SELL, HOLD, BUY
        self.q_table = {}

        self.alpha = 0.1
        self.gamma = 0.95
        self.epsilon = 0.1

        # Trading parameters
        self.initial_capital = 10000
        self.position_size = 0.1       # 10% of the capital
        self.transaction_fee = 0.001   # 0.1 %


    # -------------------- BRONZE --------------------
    def bronze_layer(self):
        schema = StructType([
            StructField("symbol", StringType()),
            StructField("price", DoubleType()),
            StructField("volume", LongType()),
            StructField("open", DoubleType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("timestamp", TimestampType())
        ])

        df = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "stock_prices") \
            .load()

        bronze = df.select(from_json(col("value").cast("string"), schema).alias("d")) \
            .select("d.*") \
            .withColumn("date", to_date("timestamp"))

        return bronze.writeStream \
            .format("delta") \
            .partitionBy("date") \
            .option("checkpointLocation", "/delta/checkpoints/bronze") \
            .start(self.bronze_path)


    # -------------------- SILVER --------------------
    def silver_layer(self):
        def process(batch, _):
            clean = batch.filter(col("price") > 0) \
                .filter(col("volume") > 0) \
                .withColumn("price_range", col("high") - col("low")) \
                .withColumn(
                    "price_change_pct",
                    (col("price") - col("open")) / col("open") * 100
                )

            clean.write.format("delta").mode("append").save(self.silver_path)

        return self.spark.readStream.format("delta") \
            .load(self.bronze_path) \
            .writeStream.foreachBatch(process) \
            .option("checkpointLocation", "/delta/checkpoints/silver") \
            .start()


    # -------------------- GOLD --------------------
    def gold_layer(self):
        def process(batch, _):
            gold = batch.groupBy("symbol").agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("price_volatility"),
                sum("volume").alias("total_volume"),
                avg("price_change_pct").alias("avg_price_change")
            )

            gold = gold \
                .withColumn(
                    "price_volatility",
                    when(
                        col("price_volatility").isNull() |
                        isnan(col("price_volatility")) |
                        (~col("price_volatility").between(-1e308, 1e308)),
                        0.0
                        ).otherwise(col("price_volatility"))
                    ) \
                    .fillna(0)
            gold.write.format("delta").mode("append").save(self.gold_path)

        return self.spark.readStream.format("delta") \
            .load(self.silver_path) \
            .writeStream.foreachBatch(process) \
            .option("checkpointLocation", "/delta/checkpoints/gold") \
            .start()

    # -------------------- MACHINE LEARNING --------------------
    def train_ml(self):
        # Load data
        df = self.spark.read.format("delta").load(self.gold_path)
        
        if df.count() < 30:
            return

        # Defensive cleanup (MANDATORY)
        df = df \
            .withColumn(
                "avg_price",
                when(isnan(col("avg_price")) | col("avg_price").isNull(), 0.0)
                .otherwise(col("avg_price"))
            ) \
            .withColumn(
                "price_volatility",
                when(isnan(col("price_volatility")) | col("price_volatility").isNull(), 0.0)
                .otherwise(col("price_volatility"))
            ) \
            .withColumn(
                "total_volume",
                when(isnan(col("total_volume")) | col("total_volume").isNull(), 0.0)
                .otherwise(col("total_volume"))
            )

        # Label
        df = df.withColumn(
            "label",
            when(col("avg_price_change") > 0, 1).otherwise(0)
        )

        # Features
        assembler = VectorAssembler(
            inputCols=["avg_price", "price_volatility", "total_volume"],
            outputCol="features",
            handleInvalid="keep"
        )
        
        data = assembler.transform(df).select("features", "label")
        train, test = data.randomSplit([0.8, 0.2], seed=42)
        
        # Training
        model = LogisticRegression().fit(train)
        auc = BinaryClassificationEvaluator().evaluate(model.transform(test))
        print(f"ML AUC = {auc:.4f}")


    # -------------------- REINFORCEMENT LEARNING --------------------
    def train_rl(self):
        df = self.spark.read.format("delta").load(self.gold_path)
        if df.count() < 30:
            return

        data = df.collect()
        portfolio = self.initial_capital

        for i in range(len(data) - 1):
            state = (
                builtins.round(float(data[i].avg_price), 2),
                builtins.round(0.0 if data[i].price_volatility is None else float(data[i].price_volatility), 2),
                builtins.round(float(data[i].total_volume), 2),
                builtins.round(float(data[i].avg_price_change), 2)
            )

            self.q_table.setdefault(state, {a: 0.0 for a in self.actions})

            # epsilon-greedy
            if random.random() < self.epsilon:
                action = random.choice(self.actions)
            else:
                action = builtins.max(self.q_table[state], key=self.q_table[state].get)

            # no trade if signal weak
            if builtins.abs(data[i].avg_price_change) < 0.5:
                action = 0

            invested = portfolio * self.position_size

            # reward financier
            reward = action * (data[i].avg_price_change / 100) * invested

            if action != 0:
                reward -= invested * self.transaction_fee

            portfolio += reward
            portfolio = builtins.max(portfolio, 0)

            next_state = (
                builtins.round(float(data[i + 1].avg_price), 2),
                builtins.round(0.0 if data[i + 1].price_volatility is None else float(data[i + 1].price_volatility), 2),
                builtins.round(float(data[i + 1].total_volume), 2),
                builtins.round(float(data[i + 1].avg_price_change), 2)
            )

            self.q_table.setdefault(next_state, {a: 0.0 for a in self.actions})
            # Q-learning update
            self.q_table[state][action] += self.alpha * (
                reward
                + self.gamma * builtins.max(self.q_table[next_state].values())
                - self.q_table[state][action]
            )

        self.spark.createDataFrame(
            [(portfolio,)],
            ["final_portfolio_value"]
        ).write.format("delta").mode("append").save(self.rl_path)

        print(f"RL completed – Final portfolio = {portfolio:.2f}")


    # -------------------- PIPELINE --------------------
    def run_complete_pipeline(self):
        b = self.bronze_layer()
        time.sleep(10)
        s = self.silver_layer()
        time.sleep(10)
        g = self.gold_layer()

        try:
            while True:
                time.sleep(60)
                self.train_ml()
                self.train_rl()
        except KeyboardInterrupt:
            b.stop()
            s.stop()
            g.stop()
            self.spark.stop()


if __name__ == "__main__":
    analyzer = DeltaLakeStockAnalyzer()
    analyzer.run_complete_pipeline()
