from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    def __init__(self):
        self.storage = []
        self.rank = 0
    @abstractmethod
    def validate(self, data:Any) -> bool:
        pass
    @abstractmethod
    def ingest(self, data:Any) -> None:
        pass

    def output(self) -> tuple[int,str]:
        if not self.storage:
            raise IndexError("No data available")
        return self.storage.pop(0)

class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int,float)):
            return True
        if isinstance(data, list):
            return all(isinstance(element, (int,float))for  element in data)
        
        return False
    
    def ingest(self,data:Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for element in data:
                self.rank += 1
                self.storage.append((self.rank, str(element)))
        else:
            self.rank += 1
            self.storage.append((self.rank, str(data)))

class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return not data.isdigit()

        if isinstance(data, list):
            return all(isinstance(element, str) and not element.isdigit() for element in data)

        return False
    
    def ingest(self,data:Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        
        if isinstance(data, list):
            for item in data:
                self.rank += 1
                self.storage.append((self.rank, item))
    

        
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
                formatted = f"{item['log_level']}: {item['log_message']}"
                self.storage.append((self.rank, formatted))

        else:
            self.rank += 1
            formatted = f"{data['log_level']}: {data['log_message']}"
            self.storage.append((self.rank, formatted))


if __name__ == "__main__":

    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    num = NumericProcessor()

    print(f"Trying to validate input '42': {num.validate(42)}")
    print(f"Trying to validate input 'Hello': {num.validate('Hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num.ingest("foo")
    except ValueError as e:
        print(f"Got exception: {e}")

    data_nums = [1, 2, 3, 4, 5]
    print(f"Processing data: {data_nums}")
    num.ingest(data_nums)

    print("Extracting 3 values...")
    for i in range(3):
        _, val = num.output()
        print(f"Numeric value {i}: {val}")

    print("\nTesting Text Processor...")
    text = TextProcessor()

    print(f"Trying to validate input '42': {text.validate('42')}")

    data_text = ["Hello", "Nexus", "World"]
    print(f"Processing data: {data_text}")
    text.ingest(data_text)

    print("Extracting 1 value...")
    _, tval = text.output()
    print(f"Text value 0: {tval}")


    print("\nTesting Log Processor...")
    log = LogProcessor()

    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")

    data_logs = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"}
    ]
    print(f"Processing data: {data_logs}")
    log.ingest(data_logs)

    print("Extracting 2 values...")
    for i in range(2):
        _, lval = log.output()
        print(f"Log entry {i}: {lval}")