from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):

    def __init__(self):
        self.storage: list[tuple[int, str]] = []
        self.rank = 0
        self.total_processed = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.storage:
            raise IndexError("No data available")
        return self.storage.pop(0)

class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return isinstance(data, (int, float)) or (
            isinstance(data, list) and all(isinstance(x, (int, float)) for x in data)
        )

    def ingest(self, data: Any) -> None:

        if not self.validate(data):
            raise ValueError("Invalid numeric data")

        if isinstance(data, list):
            for x in data:
                self.rank += 1
                self.total_processed += 1
                self.storage.append((self.rank, str(x)))
        else:
            self.rank += 1
            self.total_processed += 1
            self.storage.append((self.rank, str(data)))


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) or (
            isinstance(data, list) and all(isinstance(x, str) for x in data)
        )

    def ingest(self, data: Any) -> None:

        if not self.validate(data):
            raise ValueError("Invalid text data")

        if isinstance(data, list):
            for x in data:
                self.rank += 1
                self.total_processed += 1
                self.storage.append((self.rank, x))
        else:
            self.rank += 1
            self.total_processed += 1
            self.storage.append((self.rank, data))


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:

        if isinstance(data, dict):
            return "log_level" in data and "log_message" in data

        if isinstance(data, list):
            return all(
                isinstance(x, dict)
                and "log_level" in x
                and "log_message" in x
                for x in data
            )

        return False

    def ingest(self, data: Any) -> None:

        def fmt(d):
            return f"{d['log_level']}: {d['log_message']}"

        if not self.validate(data):
            raise ValueError("Invalid log data")

        if isinstance(data, list):
            for x in data:
                self.rank += 1
                self.total_processed += 1
                self.storage.append((self.rank, fmt(x)))
        else:
            self.rank += 1
            self.total_processed += 1
            self.storage.append((self.rank, fmt(data)))



class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...

class CSVPlugin:

    def process_output(self, data: list[tuple[int, str]]) -> None:

        values = [v for _, v in data]

        print("CSV Output:")
        print(",".join(values))

class JSONPlugin:

    def process_output(self, data: list[tuple[int, str]]) -> None:

        result = {}

        for rank, value in data:
            result[f"item_{rank}"] = value

        print("JSON Output:")
        print(result)



class DataStream:

    def __init__(self):
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:

        for element in stream:
            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    break

    def print_statistics(self) -> None:

        print("== DataStream statistics ==")

        if not self.processors:
            print("No processor found, no data")
            return

        for proc in self.processors:
            print(
                f"{proc.__class__.__name__}: "
                f"total {proc.total_processed} items processed, "
                f"remaining {len(proc.storage)} on processor"
            )


    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:

        for proc in self.processors:

            collected: list[tuple[int, str]] = []

            for _ in range(nb):
                try:
                    collected.append(proc.output())
                except IndexError:
                    break

            plugin.process_output(collected)


if __name__ == "__main__":

    print("=== Code Nexus - Data Pipeline ===")

    stream = DataStream()

    stream.print_statistics()

    print("\nRegistering Processors")

    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    stream.register_processor(num)
    stream.register_processor(text)
    stream.register_processor(log)

    batch1 = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING", "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO", "log_message": "User wil is connected"}
        ],
        42,
        ["Hi", "five"]
    ]

    print("\nSend first batch:", batch1)

    stream.process_stream(batch1)

    stream.print_statistics()

    print("\nSend 3 processed data from each processor to CSV plugin:")

    csv = CSVPlugin()
    stream.output_pipeline(3, csv)

    stream.print_statistics()

    batch2 = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {"log_level": "NOTICE", "log_message": "Certificate expires in 10 days"}
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello"
    ]

    print("\nSend second batch:", batch2)

    stream.process_stream(batch2)

    stream.print_statistics()

    print("\nSend 5 processed data from each processor to JSON plugin:")

    json = JSONPlugin()
    stream.output_pipeline(5, json)

    stream.print_statistics()