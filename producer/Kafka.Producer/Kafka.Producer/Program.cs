using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

if (args.Length != 1)
{
    Console.WriteLine("Please provide the configuration file path as a command line argument");
}

IConfiguration cfg = new ConfigurationBuilder()
    .AddIniFile(args[0])
    .Build();

const string topic = "movies";
const string csvFilePath = "C:\\Users\\38068\\source\\repos\\kafka-dotnet\\producer\\Kafka.Producer\\Kafka.Producer\\age_gaps.csv";

List<(string Movie_Name, string Release_Year)> movies = ReadMoviesFromCsv(csvFilePath);

static List<(string Movie_Name, string Release_Year)> ReadMoviesFromCsv(string csvFilePath)
{
    List<(string Movie_Name, string Release_Year)> movies = new List<(string Movie_Name, string Release_Year)>();

    // Read the CSV file
    string[] lines = File.ReadAllLines(csvFilePath);

    // Process each line in the CSV file (excluding the header)
    for (int i = 1; i < lines.Length; i++)
    {
        string line = lines[i];

        // Split the line by comma
        string[] parts = line.Split(',');

        // Extract the values and add them to the list
        string movie_Name = parts[0].Trim();
        string release_Year = parts[1].Trim();

        movies.Add((movie_Name, release_Year));
    }

    return movies;
}

CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => {
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};


var producerConfig = new ProducerConfig(cfg.Get<Dictionary<string, string>>())
{
    LingerMs = 3000,
    // BatchNumMessages = 3,
    // EnableDeliveryReports = false,
    // BatchSize = 100,
};

using var producer = new ProducerBuilder<string, string>(producerConfig.AsEnumerable()).Build();
var numProduced = 0;
var rnd = new Random();


try
{
    while (true)
    {
        var company = movies[rnd.Next(movies.Count)];

        var message = new Message<string, string>
        {
            Key = company.Release_Year,
            Value = company.Movie_Name,
        };

        var deliveryResult = await producer.ProduceAsync(topic, message, cts.Token);
        numProduced += 1;

        if (deliveryResult?.Status != PersistenceStatus.Persisted)
        {
            Console.WriteLine($"Failed to deliver message: {deliveryResult?.Status}");
            continue;
        }

        Console.WriteLine($"Produced event to topic {topic}: " +
                          $"key = {company.Release_Year,-10} value = {company.Movie_Name}");
    }
}
catch (OperationCanceledException)
{
    numProduced++;
    // Ctrl-C was pressed.
}
finally
{
    producer.Flush(TimeSpan.FromSeconds(10));
    Console.WriteLine($"{numProduced} messages were produced to topic {topic}");
}