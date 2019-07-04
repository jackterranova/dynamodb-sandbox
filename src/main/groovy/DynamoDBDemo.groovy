import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Index
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.ItemUtils
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.GetItemResult
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.Projection
import com.amazonaws.services.dynamodbv2.model.ProjectionType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType

class DynamoDBDemo {
    static void main(String[] args) {
        new DynamoDBDemo().demo()
    }

    private void demo() {
        AmazonDynamoDB ddb = createTable()
        Table perfDataTable = putItem(ddb)
        getItem1(ddb)
        getItem2(perfDataTable)
        getFromIndex(perfDataTable)
    }

    private void getFromIndex(Table perfDataTable) {
        // now read from the GSI
        Index index = perfDataTable.getIndex("RecordedAtIndex")

        def itemColl = index.scan()  //we could also perform a query on the key recorded_at

        // we have only projected video_id in to the index
        // so only recorded_at and video_id are available
        itemColl.each {
            println(it.get("recorded_at"))
            println(it.get("video_id"))
        }
    }

    private void getItem2(Table perfDataTable) {
// =======

        // this does the same as the low level ddb.getItem call
        // a litle simpler to use
        // also has the option of accepting an ItemSpec to provide more
        // control over the request, including read consistency
        Item perfDataItem2 = perfDataTable.getItem("video_id", "456")

        perfDataItem2.with {
            println(it.toJSONPretty())
            println(it.get("video_id"))
            println(it.get("recorded_at"))
            println(it.get("perf_data"))
        }
    }

    private void getItem1(AmazonDynamoDB ddb) {
// now retrieve the item we just added
        GetItemResult getItemResult = ddb.getItem(new GetItemRequest()
            .withTableName("perf_data")
            .withKey([video_id: "456"] as Map)
            .withConsistentRead(true)
        )

        // we use Dynamo SDK to translate the result to a simpler Item object
        Item perfDataItem = ItemUtils.toItem(getItemResult.item)

        // it's basically json
        perfDataItem.with {
            println(it.toJSONPretty())
            println(it.get("video_id"))
            println(it.get("recorded_at"))
            println(it.get("perf_data"))
        }
    }

    private Table putItem(AmazonDynamoDB ddb) {
        // let's add this json to the db
        Map m = [
            video_id   : "456",
            recorded_at: "2019-06-24",
            perf_data  : [
                daily_perf            : [
                    ["2019-06-24": 1],
                    ["2019-06-24": 2],
                    ["2019-06-24": 1]
                ],
                pre_window_total_views: 435
            ]
        ] as Map

        Table perfDataTable = new DynamoDB(ddb).getTable("perf_data")

        perfDataTable.putItem(new Item()
        // we need to specify the key separately
            .withPrimaryKey("video_id", m["video_id"])
        // recorded_at is basically a key, but is also a just a top level field
        // that we need to add separately from the rest of the json
            .withString("recorded_at", m["recorded_at"] as String)
        // perf_data was not predefined in the table's schema
        // it's basically attached to the primary key "video_id"
            .withMap("perf_data", m["perf_data"] as Map)
        )
        perfDataTable
    }

    private AmazonDynamoDB createTable() {
        CreateTableRequest createTableRequest = new CreateTableRequest()
            .withTableName("perf_data")

        // dynamo tables do not require attributes to be predefined
        // but keys MUST be predefined
        // video_id is primary key of the table perf_data
            .withAttributeDefinitions(
            new AttributeDefinition("video_id", ScalarAttributeType.S),
            new AttributeDefinition("recorded_at", ScalarAttributeType.S)
        )

        // keys must both be defined both as attributes and keys ...video_id
            .withKeySchema(new KeySchemaElement("video_id", KeyType.HASH))

        // create a GSI called "RecordedAtIndex" on recorded_at
            .withGlobalSecondaryIndexes(
            new GlobalSecondaryIndex()
                .withIndexName("RecordedAtIndex")
                .withKeySchema(new KeySchemaElement().withAttributeName("recorded_at").withKeyType(KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput()
                .withReadCapacityUnits((long) 10)
                .withWriteCapacityUnits((long) 1)
            )
            // we only want to project the primary key "video_id"
                .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
        )
            .withProvisionedThroughput(new ProvisionedThroughput(Long.valueOf(10), Long.valueOf(10)))

        // AmazonDynamoDB is a low level interface to DDB
        final AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder
            .newInstance()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1"))
            .build()

        try {
            ddb.createTable(createTableRequest)
        } catch (ResourceInUseException e) {
            println("table already created")
        }

        // check the tables list in the database
        println(ddb.listTables())
        println(ddb.describeTable("perf_data"))
        ddb
    }
}
