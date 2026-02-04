"""Comprehensive tests for timezone handling and DST transitions."""

import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from control_plane.app.utils.timezone import TimezoneConverter


class TestTimezoneValidation:
    """Test timezone validation."""

    def test_valid_timezone(self):
        """Test validation of valid IANA timezones."""
        assert TimezoneConverter.validate_timezone("America/New_York") is True
        assert TimezoneConverter.validate_timezone("Europe/London") is True
        assert TimezoneConverter.validate_timezone("Asia/Tokyo") is True
        assert TimezoneConverter.validate_timezone("UTC") is True

    def test_invalid_timezone(self):
        """Test validation of invalid timezones."""
        assert TimezoneConverter.validate_timezone("Invalid/Timezone") is False
        assert TimezoneConverter.validate_timezone("NotATimezone") is False
        assert TimezoneConverter.validate_timezone("") is False


class TestBasicConversions:
    """Test basic timezone conversions."""

    def test_naive_local_to_utc(self):
        """Test converting naive local time to UTC."""
        # January (EST: UTC-5)
        local_time = datetime(2024, 1, 15, 14, 30, 0)  # 2:30 PM
        utc_time = TimezoneConverter.convert_to_utc(local_time, "America/New_York")

        assert utc_time.hour == 19  # 7:30 PM UTC
        assert utc_time.minute == 30

    def test_aware_local_to_utc(self):
        """Test converting aware local time to UTC."""
        tz = ZoneInfo("America/New_York")
        local_time = datetime(2024, 1, 15, 14, 30, 0, tzinfo=tz)
        utc_time = TimezoneConverter.convert_to_utc(local_time, "America/New_York")

        assert utc_time.hour == 19
        assert utc_time.tzinfo == ZoneInfo("UTC")

    def test_utc_to_local(self):
        """Test converting UTC to local timezone."""
        utc_time = datetime(2024, 1, 15, 19, 30, 0)  # 7:30 PM UTC
        local_time = TimezoneConverter.convert_from_utc(utc_time, "America/New_York")

        assert local_time.hour == 14  # 2:30 PM EST
        assert local_time.minute == 30

    def test_round_trip_conversion(self):
        """Test converting local -> UTC -> local returns same time."""
        original = datetime(2024, 6, 15, 10, 0, 0)  # June (EDT)
        utc = TimezoneConverter.convert_to_utc(original, "America/New_York")
        back_to_local = TimezoneConverter.convert_from_utc(utc, "America/New_York")

        assert back_to_local.hour == original.hour
        assert back_to_local.minute == original.minute


class TestDSTDetection:
    """Test DST detection."""

    def test_is_dst_summer(self):
        """Test DST detection in summer (EDT)."""
        summer_time = datetime(2024, 7, 15, 12, 0, 0)  # July
        assert TimezoneConverter.is_dst(summer_time, "America/New_York") is True

    def test_is_dst_winter(self):
        """Test DST detection in winter (EST)."""
        winter_time = datetime(2024, 1, 15, 12, 0, 0)  # January
        assert TimezoneConverter.is_dst(winter_time, "America/New_York") is False

    def test_is_dst_no_dst_timezone(self):
        """Test DST detection in timezone without DST."""
        # Arizona doesn't observe DST
        time = datetime(2024, 7, 15, 12, 0, 0)
        assert TimezoneConverter.is_dst(time, "America/Phoenix") is False

    def test_utc_offset_changes_with_dst(self):
        """Test that UTC offset changes between summer and winter."""
        winter_offset = TimezoneConverter.get_utc_offset_hours(
            "America/New_York",
            datetime(2024, 1, 15, 12, 0, 0)
        )
        summer_offset = TimezoneConverter.get_utc_offset_hours(
            "America/New_York",
            datetime(2024, 7, 15, 12, 0, 0)
        )

        # EST is UTC-5, EDT is UTC-4
        assert winter_offset == -5.0
        assert summer_offset == -4.0
        assert summer_offset > winter_offset  # DST has larger offset


class TestSpringForward:
    """Test spring forward DST transition (nonexistent times)."""

    def test_detect_nonexistent_time(self):
        """Test detection of nonexistent time during spring forward."""
        # March 10, 2024: 2:00 AM -> 3:00 AM
        nonexistent = datetime(2024, 3, 10, 2, 30, 0)  # 2:30 AM doesn't exist
        assert TimezoneConverter.is_nonexistent_time(nonexistent, "America/New_York") is True

    def test_normal_time_not_nonexistent(self):
        """Test that normal times are not detected as nonexistent."""
        normal = datetime(2024, 3, 10, 1, 30, 0)  # 1:30 AM exists
        assert TimezoneConverter.is_nonexistent_time(normal, "America/New_York") is False

    def test_handle_nonexistent_shift_forward(self):
        """Test handling nonexistent time by shifting forward."""
        nonexistent = datetime(2024, 3, 10, 2, 30, 0)
        adjusted = TimezoneConverter.handle_nonexistent_time(
            nonexistent,
            "America/New_York",
            strategy="shift_forward"
        )

        # Should shift to 3:30 AM (first valid time after gap)
        assert adjusted.hour == 3
        assert adjusted.minute == 30

    def test_handle_nonexistent_shift_backward(self):
        """Test handling nonexistent time by shifting backward."""
        nonexistent = datetime(2024, 3, 10, 2, 30, 0)
        adjusted = TimezoneConverter.handle_nonexistent_time(
            nonexistent,
            "America/New_York",
            strategy="shift_backward"
        )

        # Should shift to 1:30 AM (last valid time before gap)
        assert adjusted.hour == 1
        assert adjusted.minute == 30

    def test_handle_nonexistent_raise(self):
        """Test handling nonexistent time by raising error."""
        nonexistent = datetime(2024, 3, 10, 2, 30, 0)

        with pytest.raises(ValueError, match="does not exist"):
            TimezoneConverter.handle_nonexistent_time(
                nonexistent,
                "America/New_York",
                strategy="raise"
            )

    def test_convert_nonexistent_time_to_utc(self):
        """Test converting nonexistent time to UTC (should handle gracefully)."""
        # During spring forward, 2:30 AM doesn't exist
        nonexistent = datetime(2024, 3, 10, 2, 30, 0)

        # Python's zoneinfo handles this by shifting forward
        utc_time = TimezoneConverter.convert_to_utc(nonexistent, "America/New_York")

        # Should convert successfully (implementation-dependent behavior)
        assert utc_time is not None
        assert utc_time.tzinfo == ZoneInfo("UTC")


class TestFallBack:
    """Test fall back DST transition (ambiguous times)."""

    def test_detect_ambiguous_time(self):
        """Test detection of ambiguous time during fall back."""
        # November 3, 2024: 2:00 AM -> 1:00 AM
        ambiguous = datetime(2024, 11, 3, 1, 30, 0)  # 1:30 AM happens twice
        assert TimezoneConverter.is_ambiguous_time(ambiguous, "America/New_York") is True

    def test_normal_time_not_ambiguous(self):
        """Test that normal times are not detected as ambiguous."""
        normal = datetime(2024, 11, 3, 3, 30, 0)  # 3:30 AM is unambiguous
        assert TimezoneConverter.is_ambiguous_time(normal, "America/New_York") is False

    def test_convert_ambiguous_time_first_occurrence(self):
        """Test converting ambiguous time (first occurrence - DST)."""
        ambiguous = datetime(2024, 11, 3, 1, 30, 0)

        # First occurrence (DST, before fall back)
        utc_time_first = TimezoneConverter.convert_to_utc(
            ambiguous,
            "America/New_York",
            is_dst=True
        )

        # Should be EDT (UTC-4), so 1:30 AM EDT = 5:30 AM UTC
        assert utc_time_first.hour == 5
        assert utc_time_first.minute == 30

    def test_convert_ambiguous_time_second_occurrence(self):
        """Test converting ambiguous time (second occurrence - standard)."""
        ambiguous = datetime(2024, 11, 3, 1, 30, 0)

        # Second occurrence (standard time, after fall back)
        utc_time_second = TimezoneConverter.convert_to_utc(
            ambiguous,
            "America/New_York",
            is_dst=False
        )

        # Should be EST (UTC-5), so 1:30 AM EST = 6:30 AM UTC
        assert utc_time_second.hour == 6
        assert utc_time_second.minute == 30

    def test_ambiguous_time_different_utc(self):
        """Test that ambiguous times map to different UTC times."""
        ambiguous = datetime(2024, 11, 3, 1, 30, 0)

        utc_first = TimezoneConverter.convert_to_utc(ambiguous, "America/New_York", is_dst=True)
        utc_second = TimezoneConverter.convert_to_utc(ambiguous, "America/New_York", is_dst=False)

        # They should differ by 1 hour
        assert (utc_second - utc_first).total_seconds() == 3600


class TestDSTTransitions:
    """Test DST transition detection."""

    def test_get_dst_transitions_2024(self):
        """Test getting DST transition dates for 2024."""
        spring, fall = TimezoneConverter.get_dst_transition_dates(2024, "America/New_York")

        assert spring is not None
        assert fall is not None

        # In 2024: Spring forward on March 10, Fall back on November 3
        assert spring.month == 3
        assert spring.day == 10

        assert fall.month == 11
        assert fall.day == 3

    def test_get_dst_transitions_no_dst(self):
        """Test getting DST transitions for timezone without DST."""
        spring, fall = TimezoneConverter.get_dst_transition_dates(2024, "America/Phoenix")

        # Arizona doesn't observe DST
        assert spring is None
        assert fall is None

    def test_get_dst_transitions_utc(self):
        """Test getting DST transitions for UTC (no DST)."""
        spring, fall = TimezoneConverter.get_dst_transition_dates(2024, "UTC")

        assert spring is None
        assert fall is None


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_invalid_timezone_raises_error(self):
        """Test that invalid timezone raises ValueError."""
        with pytest.raises(ValueError, match="Invalid timezone"):
            TimezoneConverter.convert_to_utc(
                datetime(2024, 1, 1, 12, 0, 0),
                "Invalid/Zone"
            )

    def test_leap_day_conversion(self):
        """Test conversion on leap day."""
        leap_day = datetime(2024, 2, 29, 12, 0, 0)
        utc_time = TimezoneConverter.convert_to_utc(leap_day, "America/New_York")

        assert utc_time.month == 2
        assert utc_time.day == 29

    def test_year_boundary_conversion(self):
        """Test conversion across year boundary."""
        new_year_eve = datetime(2023, 12, 31, 23, 30, 0)
        utc_time = TimezoneConverter.convert_to_utc(new_year_eve, "America/New_York")

        # 11:30 PM EST = 4:30 AM UTC (next day)
        assert utc_time.year == 2024
        assert utc_time.month == 1
        assert utc_time.day == 1
        assert utc_time.hour == 4
        assert utc_time.minute == 30

    def test_different_hemisphere_dst(self):
        """Test DST in southern hemisphere (opposite seasons)."""
        # Sydney: DST in January (summer), standard in July (winter)
        summer = datetime(2024, 1, 15, 12, 0, 0)
        winter = datetime(2024, 7, 15, 12, 0, 0)

        summer_is_dst = TimezoneConverter.is_dst(summer, "Australia/Sydney")
        winter_is_dst = TimezoneConverter.is_dst(winter, "Australia/Sydney")

        # In Sydney, January is summer (DST), July is winter (standard)
        assert summer_is_dst is True
        assert winter_is_dst is False


class TestMultipleTimezones:
    """Test conversions between multiple timezones."""

    def test_convert_across_multiple_timezones(self):
        """Test converting time across multiple timezones."""
        # Start with time in New York
        ny_time = datetime(2024, 6, 15, 14, 0, 0)  # 2 PM EDT

        # Convert to UTC
        utc_time = TimezoneConverter.convert_to_utc(ny_time, "America/New_York")

        # Convert to Tokyo
        tokyo_time = TimezoneConverter.convert_from_utc(utc_time, "Asia/Tokyo")

        # EDT is UTC-4, JST is UTC+9, so difference is 13 hours
        # 2 PM EDT = 6 PM UTC = 3 AM JST (next day)
        assert tokyo_time.hour == 3
        assert tokyo_time.day == 16  # Next day

    def test_convert_london_to_new_york(self):
        """Test conversion from London to New York."""
        # Summer time: BST (UTC+1), EDT (UTC-4) = 5 hour difference
        london_time = datetime(2024, 6, 15, 14, 0, 0)  # 2 PM BST

        utc = TimezoneConverter.convert_to_utc(london_time, "Europe/London")
        ny_time = TimezoneConverter.convert_from_utc(utc, "America/New_York")

        # 2 PM BST = 1 PM UTC = 9 AM EDT
        assert ny_time.hour == 9


class TestSchedulingScenarios:
    """Test real-world scheduling scenarios with DST."""

    def test_daily_schedule_across_spring_forward(self):
        """Test daily schedule that crosses spring forward transition."""
        # Schedule: Daily at 2:00 AM (before transition)
        before_transition = datetime(2024, 3, 9, 2, 0, 0)
        on_transition = datetime(2024, 3, 10, 2, 0, 0)  # This time doesn't exist!
        after_transition = datetime(2024, 3, 11, 2, 0, 0)

        # Before transition should work fine
        utc_before = TimezoneConverter.convert_to_utc(before_transition, "America/New_York")
        assert utc_before.hour == 7  # 2 AM EST = 7 AM UTC

        # On transition day, handle nonexistent time
        adjusted = TimezoneConverter.handle_nonexistent_time(
            on_transition,
            "America/New_York",
            strategy="shift_forward"
        )
        assert adjusted.hour == 3  # Shifted to 3 AM

        # After transition should work fine
        utc_after = TimezoneConverter.convert_to_utc(after_transition, "America/New_York")
        assert utc_after.hour == 6  # 2 AM EDT = 6 AM UTC

    def test_daily_schedule_across_fall_back(self):
        """Test daily schedule that crosses fall back transition."""
        # Schedule: Daily at 1:30 AM
        before_transition = datetime(2024, 11, 2, 1, 30, 0)
        on_transition = datetime(2024, 11, 3, 1, 30, 0)  # This time happens twice!
        after_transition = datetime(2024, 11, 4, 1, 30, 0)

        # Before transition
        utc_before = TimezoneConverter.convert_to_utc(before_transition, "America/New_York")
        assert utc_before.hour == 5  # 1:30 AM EDT = 5:30 AM UTC

        # On transition day - use second occurrence (after fall back)
        utc_on = TimezoneConverter.convert_to_utc(
            on_transition,
            "America/New_York",
            is_dst=False  # Use standard time (second occurrence)
        )
        assert utc_on.hour == 6  # 1:30 AM EST = 6:30 AM UTC

        # After transition
        utc_after = TimezoneConverter.convert_to_utc(after_transition, "America/New_York")
        assert utc_after.hour == 6  # 1:30 AM EST = 6:30 AM UTC

    def test_hourly_schedule_across_fall_back(self):
        """Test hourly schedule during fall back (should run twice at 1:00 AM)."""
        # During fall back, 1:00 AM occurs twice
        # First at EDT (UTC-4), then at EST (UTC-5)

        time_1am = datetime(2024, 11, 3, 1, 0, 0)

        # First occurrence (EDT)
        utc_first = TimezoneConverter.convert_to_utc(time_1am, "America/New_York", is_dst=True)
        assert utc_first.hour == 5  # 1 AM EDT = 5 AM UTC

        # Second occurrence (EST) - 1 hour later in UTC
        utc_second = TimezoneConverter.convert_to_utc(time_1am, "America/New_York", is_dst=False)
        assert utc_second.hour == 6  # 1 AM EST = 6 AM UTC

        # Verify they're 1 hour apart
        assert (utc_second - utc_first).total_seconds() == 3600


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
