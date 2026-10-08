import unittest

def valid_order(customer_id, amount):
    return customer_id is not None and amount is not None and amount > 0

def should_update(incoming_ts, stored_ts):
    return incoming_ts > stored_ts

class TestRules(unittest.TestCase):
    def test_missing_customer(self):
        self.assertFalse(valid_order(None, 100))
    def test_negative_amount(self):
        self.assertFalse(valid_order('C1', -25))
    def test_valid(self):
        self.assertTrue(valid_order('C1', 25))
    def test_late_event_not_overwrite(self):
        self.assertFalse(should_update('2026-10-07 09:00:00', '2026-10-07 10:00:00'))

if __name__ == '__main__':
    unittest.main()
