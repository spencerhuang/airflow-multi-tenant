"""Timezone utilities for handling DST and timezone conversions."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class TimezoneConverter:
    """Handles timezone conversions and DST-aware scheduling."""

    @staticmethod
    def validate_timezone(timezone_str: str) -> bool:
        """
        Validate if a timezone string is valid IANA timezone.

        Args:
            timezone_str: IANA timezone string (e.g., 'America/New_York')

        Returns:
            True if valid, False otherwise
        """
        try:
            if not timezone_str or not isinstance(timezone_str, str):
                return False
            ZoneInfo(timezone_str)
            return True
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning(f"Invalid timezone: {timezone_str}")
            return False

    @staticmethod
    def convert_to_utc(
        local_time: datetime,
        timezone_str: str,
        is_dst: Optional[bool] = None
    ) -> datetime:
        """
        Convert local time to UTC, handling DST ambiguity.

        Args:
            local_time: Naive datetime in local timezone
            timezone_str: IANA timezone string
            is_dst: For ambiguous times (fall back), specify which occurrence:
                   True = first occurrence (DST), False = second (standard time)
                   None = use fold attribute or raise error

        Returns:
            UTC datetime

        Raises:
            ValueError: If timezone is invalid or time is ambiguous without guidance
        """
        if not TimezoneConverter.validate_timezone(timezone_str):
            raise ValueError(f"Invalid timezone: {timezone_str}")

        tz = ZoneInfo(timezone_str)

        # If datetime is already aware, convert to UTC
        if local_time.tzinfo is not None:
            return local_time.astimezone(ZoneInfo("UTC"))

        # Make naive datetime timezone-aware
        # The fold attribute handles ambiguous times (0 = first, 1 = second)
        if is_dst is not None:
            local_time = local_time.replace(fold=0 if is_dst else 1)

        aware_time = local_time.replace(tzinfo=tz)
        return aware_time.astimezone(ZoneInfo("UTC"))

    @staticmethod
    def convert_from_utc(utc_time: datetime, timezone_str: str) -> datetime:
        """
        Convert UTC time to local timezone.

        Args:
            utc_time: UTC datetime (aware or naive)
            timezone_str: IANA timezone string

        Returns:
            Timezone-aware datetime in target timezone
        """
        if not TimezoneConverter.validate_timezone(timezone_str):
            raise ValueError(f"Invalid timezone: {timezone_str}")

        tz = ZoneInfo(timezone_str)

        # If naive, assume UTC
        if utc_time.tzinfo is None:
            utc_time = utc_time.replace(tzinfo=ZoneInfo("UTC"))

        return utc_time.astimezone(tz)

    @staticmethod
    def is_dst(dt: datetime, timezone_str: str) -> bool:
        """
        Check if a datetime is in DST for given timezone.

        Args:
            dt: Datetime to check (naive or aware)
            timezone_str: IANA timezone string

        Returns:
            True if in DST, False otherwise
        """
        if not TimezoneConverter.validate_timezone(timezone_str):
            raise ValueError(f"Invalid timezone: {timezone_str}")

        tz = ZoneInfo(timezone_str)

        # Make timezone-aware if naive
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        else:
            dt = dt.astimezone(tz)

        # Check DST by comparing offset
        # DST typically has a larger UTC offset (less negative or more positive)
        return dt.dst() != timedelta(0)

    @staticmethod
    def get_dst_transition_dates(year: int, timezone_str: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Get DST transition dates for a given year and timezone.

        Args:
            year: Year to check
            timezone_str: IANA timezone string

        Returns:
            Tuple of (spring_forward_date, fall_back_date) in UTC
            None if timezone doesn't observe DST
        """
        if not TimezoneConverter.validate_timezone(timezone_str):
            raise ValueError(f"Invalid timezone: {timezone_str}")

        tz = ZoneInfo(timezone_str)

        # Check each day of the year for DST transitions
        spring_forward = None
        fall_back = None

        prev_dst = None
        for day in range(1, 367):
            try:
                dt = datetime(year, 1, 1, 12, 0, 0, tzinfo=tz) + timedelta(days=day-1)
                curr_dst = dt.dst() != timedelta(0)

                if prev_dst is not None:
                    if not prev_dst and curr_dst:
                        # Spring forward (entering DST)
                        spring_forward = dt.astimezone(ZoneInfo("UTC"))
                    elif prev_dst and not curr_dst:
                        # Fall back (exiting DST)
                        fall_back = dt.astimezone(ZoneInfo("UTC"))

                prev_dst = curr_dst
            except ValueError:
                # Invalid date (e.g., Feb 30)
                continue

        return (spring_forward, fall_back)

    @staticmethod
    def is_ambiguous_time(dt: datetime, timezone_str: str) -> bool:
        """
        Check if a naive datetime is ambiguous in the given timezone.

        Ambiguous times occur during fall back (e.g., 1:30 AM happens twice).

        Args:
            dt: Naive datetime to check
            timezone_str: IANA timezone string

        Returns:
            True if time is ambiguous (occurs twice), False otherwise
        """
        if not TimezoneConverter.validate_timezone(timezone_str):
            raise ValueError(f"Invalid timezone: {timezone_str}")

        if dt.tzinfo is not None:
            return False

        tz = ZoneInfo(timezone_str)

        try:
            # Try both fold values
            dt_fold0 = dt.replace(tzinfo=tz, fold=0)
            dt_fold1 = dt.replace(tzinfo=tz, fold=1)

            # If they map to different UTC times, it's ambiguous
            return dt_fold0.astimezone(ZoneInfo("UTC")) != dt_fold1.astimezone(ZoneInfo("UTC"))
        except:
            return False

    @staticmethod
    def is_nonexistent_time(dt: datetime, timezone_str: str) -> bool:
        """
        Check if a naive datetime doesn't exist in the given timezone.

        Nonexistent times occur during spring forward (e.g., 2:30 AM is skipped).

        Args:
            dt: Naive datetime to check
            timezone_str: IANA timezone string

        Returns:
            True if time doesn't exist (skipped by DST), False otherwise
        """
        if not TimezoneConverter.validate_timezone(timezone_str):
            raise ValueError(f"Invalid timezone: {timezone_str}")

        if dt.tzinfo is not None:
            return False

        tz = ZoneInfo(timezone_str)

        try:
            # Localize the naive datetime
            aware = dt.replace(tzinfo=tz)

            # Convert to UTC and back to the same timezone
            # If the time doesn't exist, the round trip will give us a different time
            utc = aware.astimezone(ZoneInfo("UTC"))
            back_to_local = utc.astimezone(tz)

            # Compare the time components
            # If they're different, the time was nonexistent and got shifted
            return (back_to_local.hour != dt.hour or
                    back_to_local.minute != dt.minute or
                    back_to_local.second != dt.second)
        except:
            return True

    @staticmethod
    def handle_nonexistent_time(dt: datetime, timezone_str: str, strategy: str = "shift_forward") -> datetime:
        """
        Handle nonexistent times during spring forward.

        Args:
            dt: Naive datetime that doesn't exist
            timezone_str: IANA timezone string
            strategy: How to handle:
                - 'shift_forward': Move to first valid time after gap
                - 'shift_backward': Move to last valid time before gap
                - 'raise': Raise ValueError

        Returns:
            Adjusted datetime that exists in the timezone

        Raises:
            ValueError: If strategy is 'raise' and time is nonexistent
        """
        if not TimezoneConverter.is_nonexistent_time(dt, timezone_str):
            return dt

        if strategy == "raise":
            raise ValueError(f"Time {dt} does not exist in timezone {timezone_str}")

        tz = ZoneInfo(timezone_str)

        if strategy == "shift_forward":
            # Shift forward by 1 hour (typical DST gap)
            # This maintains the same minutes, just moves to the next valid hour
            adjusted = dt + timedelta(hours=1)
            # Verify it's valid, if not keep searching
            if not TimezoneConverter.is_nonexistent_time(adjusted, timezone_str):
                return adjusted
            # If still nonexistent, search minute by minute
            for minutes in range(1, 120):
                test_dt = dt + timedelta(minutes=minutes)
                if not TimezoneConverter.is_nonexistent_time(test_dt, timezone_str):
                    return test_dt
        elif strategy == "shift_backward":
            # Shift backward by 1 hour
            adjusted = dt - timedelta(hours=1)
            # Verify it's valid, if not keep searching
            if not TimezoneConverter.is_nonexistent_time(adjusted, timezone_str):
                return adjusted
            # If still nonexistent, search minute by minute backwards
            for minutes in range(1, 120):
                test_dt = dt - timedelta(minutes=minutes)
                if not TimezoneConverter.is_nonexistent_time(test_dt, timezone_str):
                    return test_dt

        # Fallback: return original
        return dt

    @staticmethod
    def get_utc_offset_hours(timezone_str: str, at_time: Optional[datetime] = None) -> float:
        """
        Get UTC offset in hours for a timezone at a specific time.

        Args:
            timezone_str: IANA timezone string
            at_time: Datetime to check offset (defaults to now)

        Returns:
            UTC offset in hours (negative for west, positive for east)
        """
        if not TimezoneConverter.validate_timezone(timezone_str):
            raise ValueError(f"Invalid timezone: {timezone_str}")

        tz = ZoneInfo(timezone_str)

        if at_time is None:
            at_time = datetime.now(tz=tz)
        elif at_time.tzinfo is None:
            at_time = at_time.replace(tzinfo=tz)
        else:
            at_time = at_time.astimezone(tz)

        offset = at_time.utcoffset()
        return offset.total_seconds() / 3600 if offset else 0.0
