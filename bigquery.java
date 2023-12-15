import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.WithTimestamps;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DataflowToBigQueryExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // Simulated data
        CustomTableRow customTableRow = new CustomTableRow();
        customTableRow.setDateField("2023-12-15");
        customTableRow.setTimestampField("2023-12-15T12:30:45");

        // Convert custom TableRow to TableRow
        pipeline
            .apply("Create TableRow", MapElements
                .into(TypeDescriptor.of(TableRow.class))
                .via((CustomTableRow input) -> {
                    TableRow row = new TableRow();
                    row.set("dateField", input.get("dateField"));
                    row.set("timestampField", input.get("timestampField"));
                    return row;
                }))
            .apply("Write to BigQuery", BigQueryIO.writeTableRows()
                .to("your-project:your-dataset.your-table")
                .withSchema(getTableSchema())
                .withFormatFunction((TableRow row) -> row));

        // Run the pipeline
        pipeline.run();
    }

    // Define the BigQuery table schema
    private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = Arrays.asList(
            new TableFieldSchema().setName("dateField").setType("DATE"),
            new TableFieldSchema().setName("timestampField").setType("TIMESTAMP")
        );

        return new TableSchema().setFields(fields);
    }
}
