from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union


class DataClientInterface(ABC):
    """Abstract interface for data clients to ensure both QBench and Local API clients have the same contract."""
    
    @abstractmethod
    def fetch_recent_samples(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
    ) -> List[Dict[str, Any]]:
        """Fetch samples within a given date range."""
        pass
    
    @abstractmethod
    def count_recent_tests(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
        sample_ids: Optional[Sequence[Union[str, int]]] = None,
        chunk_size: int = 100,
        previous_range: Optional[Tuple[Optional[datetime], Optional[datetime]]] = None,
    ) -> Tuple[
        int,
        List[Tuple[datetime, int]],
        float,
        int,
        List[Tuple[datetime, float, int]],
        List[Tuple[datetime, float, int]],
    ]:
        """Collect tests created within a date range and optionally include a comparison period."""
        pass
    
    @abstractmethod
    def count_recent_customers(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
    ) -> int:
        """Count customers created within the given date range."""
        pass
    
    @abstractmethod
    def fetch_recent_customers(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
    ) -> List[Dict[str, Any]]:
        """Fetch customers created within the given date range."""
        pass
    
    @abstractmethod
    def fetch_recent_orders(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        page_size: int = 50,
        max_days: Optional[int] = None,
        default_days: int = 7,
    ) -> List[Dict[str, Any]]:
        """Fetch orders within a given date range."""
        pass
    
    @abstractmethod
    def fetch_customer_details(self, customer_id: Union[str, int]) -> Optional[Dict[str, Any]]:
        """Fetch details for a specific customer."""
        pass