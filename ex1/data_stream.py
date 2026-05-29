from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):

    def __init__(self):
        self.storage = []
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
        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self.rank += 1
                self.total_processed += 1
                self.storage.append((self.rank, str(item)))
        else:
            self.rank += 1
            self.total_processed += 1
            self.storage.append((self.rank, str(data)))

class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True

        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Invalid text data")

        if isinstance(data, list):
            for item in data:
                self.rank += 1
                self.total_processed += 1
                self.storage.append((self.rank, item))
        else:
            self.rank += 1
            self.total_processed += 1
            self.storage.append((self.rank, data))


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return (
                "log_level" in data
                and "log_message" in data
                and isinstance(data["log_level"], str)
                and isinstance(data["log_message"], str)
            )

        if isinstance(data, list):
            return all(
                isinstance(item, dict)
                and "log_level" in item
                and "log_message" in item
                and isinstance(item["log_level"], str)
                and isinstance(item["log_message"], str)
                for item in data
            )

        return False

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Invalid log data")

        if isinstance(data, list):
            for item in data:
                self.rank += 1
                self.total_processed += 1

                formatted = (
                    f"{item['log_level']}: "
                    f"{item['log_message']}"
                )

                self.storage.append((self.rank, formatted))

        else:
            self.rank += 1
            self.total_processed += 1

            formatted = (
                f"{data['log_level']}: "
                f"{data['log_message']}"
            )

            self.storage.append((self.rank, formatted))


class DataStream:

    def __init__(self):
        self.processors = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:

        for element in stream:

            processed = False

            for proc in self.processors:

                if proc.validate(element):
                    proc.ingest(element)
                    processed = True
                    break

            if not processed:
                print(
                    f"DataStream error - "
                    f"Can't process element in stream: {element}"
                )

    def print_processors_stats(self) -> None:

        print("== DataStream statistics ==")

        if not self.processors:
            print("No processor found, no data")
            return

        for proc in self.processors:

            if isinstance(proc, NumericProcessor):
                name = "Numeric Processor"

            elif isinstance(proc, TextProcessor):
                name = "Text Processor"

            elif isinstance(proc, LogProcessor):
                name = "Log Processor"

            else:
                name = "Unknown Processor"

            print(
                f"{name}: total "
                f"{proc.total_processed} items processed, "
                f"remaining {len(proc.storage)} on processor"
            )



if __name__ == "__main__":

    print("=== Code Nexus - Data Stream ===")

    print("Initialize Data Stream...")

    stream = DataStream()

    stream.print_processors_stats()


    print("\nRegistering Numeric Processor")

    num = NumericProcessor()

    stream.register_processor(num)

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead"
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected"
            }
        ],
        42,
        ["Hi", "five"]
    ]

    print("\nSend first batch of data on stream:")
    print(batch)

    stream.process_stream(batch)

    stream.print_processors_stats()


    print("\nRegistering other data processors")

    text = TextProcessor()
    log = LogProcessor()

    stream.register_processor(text)
    stream.register_processor(log)

    print("\nSend the same batch again")

    stream.process_stream(batch)

    stream.print_processors_stats()


    print("\nConsume some elements from the data processors: Numeric 3, Text 2, Log 1")
    num.output()
    num.output()
    num.output()

    text.output()
    text.output()

    log.output()

    print()

    stream.print_processors_stats()