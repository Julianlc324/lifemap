import pytest
pytest.importorskip('kafka')
from datetime import datetime
from streaming.producer import parse_args, to_message


@pytest.mark.parametrize('args', [['--days-per-tick', '0'], ['--days-per-tick', 'nan'],
                                 ['--tick-seconds', '-1'], ['--limit', '-1']])
def test_invalid_clock_arguments(args):
    with pytest.raises(SystemExit):
        parse_args(args)


def test_message_preserves_source_id_and_event_time():
    doc = {'_id': 'r1', 'business_id': 'b1', 'stars': 2, 'text': 'bad',
           'date': datetime(2020, 7, 1, 0, 0, 0, 123456)}
    message = to_message(doc)
    assert message['review_id'] == 'r1'
    assert message['date'] == '2020-07-01T00:00:00.123456'
    assert message['business_id'] == 'b1'
