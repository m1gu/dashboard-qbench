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

    @abstractmethod
    def fetch_order_throughput(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        interval: str = "week",
    ) -> Dict[str, Any]:
        """Fetch throughput analytics for orders within the given range."""
        pass

    @abstractmethod
    def fetch_sample_cycle_time(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        interval: str = "day",
    ) -> Dict[str, Any]:
        """Fetch cycle-time analytics for completed samples."""
        pass

    @abstractmethod
    def fetch_order_funnel(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Fetch funnel analytics that shows counts per stage."""
        pass

    @abstractmethod
    def fetch_slowest_orders(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        *,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Fetch or derive the slowest orders for the given date range."""
        pass

    @abstractmethod
    def fetch_overdue_orders(
        self,
        *,
        date_from: Optional[datetime] = None,
        date_to: Optional[datetime] = None,
        min_days_overdue: int = 5,
        sla_hours: int = 240,
        top_limit: int = 50,
    ) -> Dict[str, Any]:
        """Fetch overdue orders analytics used for prioritizing work."""
        pass
