from eneel.printer import *


def test_get_color():

    assert get_color('red') == COLOR_FG_RED
    assert get_color('green') == COLOR_FG_GREEN
    assert get_color('yellow') == COLOR_FG_YELLOW
    assert get_color('undefined') == ''


def test_get_timstamp():

    assert len(get_timestamp()) == 8
