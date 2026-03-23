import unittest

import utils as tu

class TestCheckoutOrchestrator(unittest.TestCase):

    def test_checkout_success(self):
        """Happy path: sufficient credit and stock."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 50)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 3)

        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        # credit should be reduced by 3 * 10 = 30
        credit = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 70)

        # stock should be reduced by 3
        stock = tu.find_item(item_id)['stock']
        self.assertEqual(stock, 47)

    def test_checkout_insufficient_credit(self):
        """Payment fails first — nothing to compensate."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 5)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 50)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 1)

        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

        # credit untouched — payment was never reserved
        credit = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 5)

        # stock untouched — orchestrator never reached stock step
        stock = tu.find_item(item_id)['stock']
        self.assertEqual(stock, 50)

    def test_checkout_insufficient_stock_compensates_payment(self):
        """Payment succeeds but stock fails — payment must be rolled back."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 2)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 5)  # wants 5, only 2 in stock

        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

        # credit must be fully restored — compensation happened
        credit = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 100)

        # stock untouched — reservation was rejected
        stock = tu.find_item(item_id)['stock']
        self.assertEqual(stock, 2)

    def test_checkout_multiple_items_one_out_of_stock(self):
        """Two items in order, second has no stock — full rollback."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        item1 = tu.create_item(10)
        item_id1 = item1['item_id']
        tu.add_stock(item_id1, 50)

        item2 = tu.create_item(20)
        item_id2 = item2['item_id']
        # deliberately no stock added for item2

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id1, 1)
        tu.add_item_to_order(order_id, item_id2, 1)

        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

        # payment compensated
        credit = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 100)

        # stock unchanged for both items
        self.assertEqual(tu.find_item(item_id1)['stock'], 50)
        self.assertEqual(tu.find_item(item_id2)['stock'], 0)

    def test_checkout_order_not_found(self):
        """Checkout with a non-existent order id."""
        checkout_response = tu.checkout_order("nonexistent-order-id")
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

    def test_checkout_no_credit(self):
        """User exists but has zero credit."""
        user = tu.create_user()
        user_id = user['user_id']
        # no credit added

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 50)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 1)

        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

        # nothing changed
        self.assertEqual(tu.find_user(user_id)['credit'], 0)
        self.assertEqual(tu.find_item(item_id)['stock'], 50)

    def test_checkout_retry_after_failure(self):
        """First checkout fails due to no credit, second succeeds after adding funds."""
        user = tu.create_user()
        user_id = user['user_id']

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 50)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 2)

        # first attempt — no credit
        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_response.status_code))

        self.assertEqual(tu.find_user(user_id)['credit'], 0)
        self.assertEqual(tu.find_item(item_id)['stock'], 50)

        # add funds and retry
        tu.add_credit_to_user(user_id, 50)
        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        self.assertEqual(tu.find_user(user_id)['credit'], 30)
        self.assertEqual(tu.find_item(item_id)['stock'], 48)

    def test_checkout_exact_credit(self):
        """Credit exactly matches order total — should succeed with zero remaining."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 30)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 50)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 3)  # total = 30

        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        self.assertEqual(tu.find_user(user_id)['credit'], 0)
        self.assertEqual(tu.find_item(item_id)['stock'], 47)

    def test_checkout_exact_stock(self):
        """Stock exactly matches requested quantity — should succeed with zero remaining."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 3)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 3)

        checkout_response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        self.assertEqual(tu.find_user(user_id)['credit'], 70)
        self.assertEqual(tu.find_item(item_id)['stock'], 0)

if __name__ == '__main__':
    unittest.main()