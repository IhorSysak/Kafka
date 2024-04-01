using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;

IConfiguration cfg = new ConfigurationBuilder()
    .AddIniFile(args[0])
    .Build();

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => {
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

var config = new StreamConfig<StringSerDes, StringSerDes>
{
    ApplicationId = "stream-app",
    BootstrapServers = "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "FCN5FNVE6Z5QBZE7",
    SaslPassword = "aA0kXgbq1Vj7tKrxmySyfvi4F4AIxvof9S7yvDl152UoQ+BJhdc4/Jzh5umGybxf",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

var builder = new StreamBuilder();
const string topicIn = "movies";

(string film1970to1990, string film1990to2000, string film2000to2020) filmTopics =
    ("films1970to1990", "films1990to2000", "films2000to2020");

var branches = builder.Stream<string, string>(topicIn)
    .Branch(
        (k, _) => int.Parse(k) >= 1970 && int.Parse(k) < 1990,
        (k, _) => int.Parse(k) >= 1990 && int.Parse(k) < 2000,
        (k, _) => int.Parse(k) >= 2000 && int.Parse(k) <= 2023);

var i = 0;
branches[i++].To(filmTopics.film1970to1990);
branches[i++].To(filmTopics.film1990to2000);
branches[i++].To(filmTopics.film2000to2020);


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
