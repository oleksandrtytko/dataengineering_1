import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class App {

	private static final String EVENT_FIELD = "type";
	private static final String PUSH_EVENT = "PushEvent";
	private static final String PAYLOAD_OBJECT_FIELD = "payload";
	private static final String COMMITS_FIELD = "commits";
	private static final String AUTHOR_OBJECT_FIELD = "author";
	private static final String AUTHOR_NAME_FIELD = "name";
	private static final String COMMIT_MESSAGE_FIELD = "message";
	private static final String THREE_GRAMS_DELIMITER = " ";
	private static final String SAVE_FORMAT = "csv";
	private static final String RESULT_PATH = "results.csv";
	private static final String[] CSV_COLUMN_NAMES = {"author", "3-gram 1", "3-gram 2", "3-gram 3"};

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder()
				.appName("App").config("spark.master", "local")
				.getOrCreate();

		JavaRDD<Row> json = spark.read()
				.json("10K.github.json")
				.toJavaRDD();

		StructType schema = DataTypes.createStructType(
				new StructField[]{
						DataTypes.createStructField(CSV_COLUMN_NAMES[0], DataTypes.StringType, true),
						DataTypes.createStructField(CSV_COLUMN_NAMES[1], DataTypes.StringType, true),
						DataTypes.createStructField(CSV_COLUMN_NAMES[2], DataTypes.StringType, true),
						DataTypes.createStructField(CSV_COLUMN_NAMES[3], DataTypes.StringType, true)
				});

		spark.createDataFrame(
				json.filter(githubCommit -> PUSH_EVENT.equals(githubCommit.getAs(EVENT_FIELD)))
						.filter(githubCommit -> {
							Row payloadRow = githubCommit.getAs(PAYLOAD_OBJECT_FIELD);
							List<Row> commits = payloadRow.getList(payloadRow.fieldIndex(COMMITS_FIELD));
							return commits != null && !commits.isEmpty();
						})
						.flatMapToPair(githubCommit -> {
							Row payloadRow = githubCommit.getAs(PAYLOAD_OBJECT_FIELD);
							List<Row> commits = payloadRow.getList(payloadRow.fieldIndex(COMMITS_FIELD));
							return commits.stream()
									.map(r -> new Tuple2<String, List<String>>(((Row) r.getAs(AUTHOR_OBJECT_FIELD)).getAs(AUTHOR_NAME_FIELD),
											generate3Grams(r.getAs(COMMIT_MESSAGE_FIELD))))
									.collect(Collectors.toList()).listIterator();
						})
						.filter(threeGram -> threeGram._2() != null)
						.map(tuple -> RowFactory.create(tuple._1(), tuple._2().get(0), tuple._2().get(1), tuple._2().get(2)))
						.rdd(),
				schema)
		.write()
		.format(SAVE_FORMAT)
		.option("header", true)
		.save(RESULT_PATH);

		spark.cloneSession();
	}

	private static List<String> generate3Grams(String input) {

		List<String> words = Arrays.stream(removePunctuation(input).toLowerCase().split(" "))
				.filter(StringUtils::isNotBlank)
				.collect(Collectors.toList());

		if (words.size() < 5) {
			return null;
		}

		return Stream.iterate(0, (p) -> p + 1)
				.limit(3)
				.map(i -> String.join(THREE_GRAMS_DELIMITER, words.subList(i, i + 3)))
				.collect(Collectors.toList());

	}

	private static String removePunctuation(String input) {
		return input.replaceAll("[^\\s\\p{L}0-9]", "").replaceAll("\n", "");
	}
}
