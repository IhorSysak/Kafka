using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

IConfiguration cfg = new ConfigurationBuilder()
    .AddIniFile(args[0])
    .Build();

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => {
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

var config = new StreamConfig<StringSerDes, StringSerDes>()
{
    ApplicationId = "stream-app",
    BootstrapServers = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "FCN5FNVE6Z5QBZE7",
    SaslPassword = "aA0kXgbq1Vj7tKrxmySyfvi4F4AIxvof9S7yvDl152UoQ+BJhdc4/Jzh5umGybxf",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    ReplicationFactor = 3,
};


var builder = new StreamBuilder();
(string films1970to1990, string films1990to2000, string films2000to2020) filmsTopics =
(
    "films1970to1990", "films1990to2000", "films2000to2020"
);

new List<IKStream<string, string>>
    {
        builder.Stream<string, string>(filmsTopics.films1970to1990),
        builder.Stream<string, string>(filmsTopics.films1990to2000),
        builder.Stream<string, string>(filmsTopics.films2000to2020),
    }.Aggregate((a, b) => a.Merge(b))
    .GroupBy((k, v) =>
    {
        int releaseYear = int.Parse(k);
        Console.WriteLine($"Current: {releaseYear} - {v}");
        if (releaseYear >= 1900 && releaseYear < 1990)
        {
            Console.WriteLine($"Result: 1900 - 1990 - {v}");
            return filmsTopics.films1970to1990;
        }
        else if (releaseYear >= 1990 && releaseYear < 2000)
        {
            Console.WriteLine($"Result: 1990 - 2000 - {v}");
            return filmsTopics.films1990to2000;
        }
        else if (releaseYear >= 2000 && releaseYear <= 2023)
        {
            Console.WriteLine($"Result: 2000 - 2023 - {v}");
            return filmsTopics.films2000to2020;
        }
        else
        {
            Console.WriteLine($"Unknown - {v}");
            return "Unknown";
        }
    })
    .Count(RocksDb.As<string, long>("count-store"))
    .MapValues((k, v) => Convert.ToString(v))
    .ToStream().To("counters");

var topology = builder.Build();
var stream = new KafkaStream(topology, config);

try
{
    await stream.StartAsync(cts.Token);
}
catch (OperationCanceledException)
{
    // Ctrl-C was pressed.
    stream.Dispose();
}

