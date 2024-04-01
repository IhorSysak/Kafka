using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

if (args.Length != 1)
{
    Console.WriteLine("Please provide the configuration file path as a command line argument");
}

IConfiguration cfg = new ConfigurationBuilder()
    .AddIniFile(args[0])
    .Build();

cfg["group.id"] = "kafka-dotnet-getting-started";
cfg["auto.offset.reset"] = "earliest";

const string topic = "counters"; //output_movies

CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => {
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

var consumerConfig = new ConsumerConfig(cfg.Get<Dictionary<string, string>>())
{
    EnableAutoCommit = true,
    EnableAutoOffsetStore = false,
};

using var consumer = new ConsumerBuilder<string, string>(consumerConfig.AsEnumerable()).Build();
var numConsumed = 0;

consumer.Subscribe(topic);
try
{
    while (true)
    {
        var cr = consumer.Consume(cts.Token);
        numConsumed++;
        Console.WriteLine($"Consumed event from topic {topic} with key {cr.Message.Key,-10} and value {cr.Message.Value}");
    }
}
catch (OperationCanceledException)
{
    // Ctrl-C was pressed.
}
finally
{
    consumer.Close();
    Console.WriteLine($"{numConsumed} messages were consumed from topic {topic}");
}