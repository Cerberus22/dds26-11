import time
import unittest

import utils as tu


class Test2PC(unittest.TestCase):

    def test_2pc_commit(self):
        """Both stock and payment prepare and commit successfully."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 2)  # costs 20

        # verify state before
        self.assertEqual(tu.find_item(item_id)['stock'], 5)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)

        response = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(response.status_code))

        paid = tu.wait_for_order_paid(order_id, timeout=5)

        # verify state after, both must have changed atomically
        self.assertTrue(paid)
        self.assertEqual(tu.find_item(item_id)['stock'], 3)
        self.assertEqual(tu.find_user(user_id)['credit'], 80)


    def test_2pc_abort_insufficient_stock(self):
        """Stock prepare fails, payment must never be touched."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 1)  # only 1 in stock

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 5)  # trying to buy 5

        response = tu.checkout_order(order_id)

        tu.wait_for_order_not_paid(order_id, timeout=5)

        # nothing should have changed
        self.assertEqual(tu.find_item(item_id)['stock'], 1)
        self.assertEqual(tu.find_user(user_id)['credit'], 100)
        self.assertFalse(tu.find_order(order_id)['paid'])

    def test_2pc_abort_insufficient_credit(self):
        """Stock prepare succeeds, payment prepare fails, stock must be fully released."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 5)  # not enough

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 2)  # costs 20, user only has 5

        response = tu.checkout_order(order_id)
        # self.assertTrue(tu.status_code_is_failure(response.status_code))

        paid = tu.wait_for_order_paid(order_id, timeout=5)

        # stock must be fully restored, this is the key 2PC guarantee
        self.assertEqual(tu.find_item(item_id)['stock'], 5)
        self.assertEqual(tu.find_user(user_id)['credit'], 5)
        self.assertFalse(paid)


    def test_2pc_idempotent_commit(self):
        """Committing the same transaction twice should not double deduct.
        This is achieved by the "already committeed" messages."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 5)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 1)  # costs 10

        tu.checkout_order(order_id)
        paid = tu.wait_for_order_paid(order_id, timeout=5)

        # simulate recovery re-sending commit by calling commit endpoints directly
        # this should be a no-op, not double deduct
        # check that stock and credit are correct after a second checkout attempt
        second_response = tu.checkout_order(order_id)
        tu.wait_for_order_paid(order_id, timeout=5)

        # stock and credit must not change again
        self.assertEqual(tu.find_item(item_id)['stock'], 4)
        self.assertEqual(tu.find_user(user_id)['credit'], 90)

    def test_2pc_multiple_orders_same_user(self):
        """Multiple orders from the same user, all should succeed if enough credit and stock."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 100)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 10)

        # create 3 orders, each buying 2 items (cost 20 each)
        order_ids = []
        for _ in range(3):
            order = tu.create_order(user_id)
            order_id = order['order_id']
            tu.add_item_to_order(order_id, item_id, 2)
            order_ids.append(order_id)

        # checkout all 3
        for order_id in order_ids:
            tu.checkout_order(order_id)

        # wait for all to complete
        for order_id in order_ids:
            self.assertTrue(tu.wait_for_order_paid(order_id, timeout=10))

        # 3 orders x 2 items = 6 stock used, 3 x 20 = 60 credit used
        self.assertEqual(tu.find_item(item_id)['stock'], 4)
        self.assertEqual(tu.find_user(user_id)['credit'], 40)

    def test_2pc_multiple_orders_different_users(self):
        """Different users each checkout their own order, all succeed."""
        item = tu.create_item(5)
        item_id = item['item_id']
        tu.add_stock(item_id, 20)

        order_ids = []
        user_ids = []
        for _ in range(4):
            user = tu.create_user()
            user_id = user['user_id']
            tu.add_credit_to_user(user_id, 50)
            user_ids.append(user_id)

            order = tu.create_order(user_id)
            order_id = order['order_id']
            tu.add_item_to_order(order_id, item_id, 3)  # costs 15
            order_ids.append(order_id)

        # checkout all
        for order_id in order_ids:
            tu.checkout_order(order_id)

        for order_id in order_ids:
            self.assertTrue(tu.wait_for_order_paid(order_id, timeout=10))

        # 4 orders x 3 items = 12 stock used
        self.assertEqual(tu.find_item(item_id)['stock'], 8)
        # each user spent 15
        for user_id in user_ids:
            self.assertEqual(tu.find_user(user_id)['credit'], 35)

    def test_2pc_contention_last_stock(self):
        """Two users race for the last item. Exactly one should succeed."""
        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 1)  # only 1 in stock

        user1 = tu.create_user()
        user1_id = user1['user_id']
        tu.add_credit_to_user(user1_id, 100)

        user2 = tu.create_user()
        user2_id = user2['user_id']
        tu.add_credit_to_user(user2_id, 100)

        order1 = tu.create_order(user1_id)
        order1_id = order1['order_id']
        tu.add_item_to_order(order1_id, item_id, 1)

        order2 = tu.create_order(user2_id)
        order2_id = order2['order_id']
        tu.add_item_to_order(order2_id, item_id, 1)

        # checkout both simultaneously
        tu.checkout_order(order1_id)
        tu.checkout_order(order2_id)

        time.sleep(5)

        order1_paid = tu.find_order(order1_id)['paid']
        order2_paid = tu.find_order(order2_id)['paid']

        # exactly one must succeed
        self.assertEqual(order1_paid + order2_paid, 1, "Exactly one order should have been paid")

        # stock must be 0
        self.assertEqual(tu.find_item(item_id)['stock'], 0)

        # only the successful user should have been charged
        user1_credit = tu.find_user(user1_id)['credit']
        user2_credit = tu.find_user(user2_id)['credit']
        if order1_paid:
            self.assertEqual(user1_credit, 90)
            self.assertEqual(user2_credit, 100)
        else:
            self.assertEqual(user1_credit, 100)
            self.assertEqual(user2_credit, 90)

    def test_2pc_contention_last_credit(self):
        """One user with limited credit, two orders race. Exactly one should succeed."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 10)  # only enough for one order

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 10)  # plenty of stock

        order1 = tu.create_order(user_id)
        order1_id = order1['order_id']
        tu.add_item_to_order(order1_id, item_id, 1)  # costs 10

        order2 = tu.create_order(user_id)
        order2_id = order2['order_id']
        tu.add_item_to_order(order2_id, item_id, 1)  # costs 10

        tu.checkout_order(order1_id)
        tu.checkout_order(order2_id)

        time.sleep(5)

        order1_paid = tu.find_order(order1_id)['paid']
        order2_paid = tu.find_order(order2_id)['paid']

        # exactly one must succeed
        self.assertEqual(order1_paid + order2_paid, 1, "Exactly one order should have been paid")

        # credit must be 0, stock reduced by 1
        self.assertEqual(tu.find_user(user_id)['credit'], 0)
        self.assertEqual(tu.find_item(item_id)['stock'], 9)

    def test_2pc_multiple_items_in_order(self):
        """Order with multiple different items, all must be deducted atomically."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 200)

        item1 = tu.create_item(10)
        item1_id = item1['item_id']
        tu.add_stock(item1_id, 5)

        item2 = tu.create_item(20)
        item2_id = item2['item_id']
        tu.add_stock(item2_id, 5)

        item3 = tu.create_item(30)
        item3_id = item3['item_id']
        tu.add_stock(item3_id, 5)

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item1_id, 2)  # costs 20
        tu.add_item_to_order(order_id, item2_id, 1)  # costs 20
        tu.add_item_to_order(order_id, item3_id, 1)  # costs 30

        # total cost = 70
        tu.checkout_order(order_id)
        self.assertTrue(tu.wait_for_order_paid(order_id, timeout=10))

        self.assertEqual(tu.find_item(item1_id)['stock'], 3)
        self.assertEqual(tu.find_item(item2_id)['stock'], 4)
        self.assertEqual(tu.find_item(item3_id)['stock'], 4)
        self.assertEqual(tu.find_user(user_id)['credit'], 130)

    def test_2pc_partial_stock_failure_multi_item(self):
        """Order with multiple items, one item has insufficient stock. Nothing should change."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 200)

        item1 = tu.create_item(10)
        item1_id = item1['item_id']
        tu.add_stock(item1_id, 5)

        item2 = tu.create_item(20)
        item2_id = item2['item_id']
        tu.add_stock(item2_id, 0)  # no stock!

        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item1_id, 2)  # would be fine
        tu.add_item_to_order(order_id, item2_id, 1)  # will fail

        tu.checkout_order(order_id)

        paid = tu.wait_for_order_paid(order_id, timeout=5)

        # nothing should have changed
        self.assertFalse(paid)
        self.assertEqual(tu.find_item(item1_id)['stock'], 5)
        self.assertEqual(tu.find_item(item2_id)['stock'], 0)
        self.assertEqual(tu.find_user(user_id)['credit'], 200)

    def test_2pc_sequential_checkouts_drain_stock(self):
        """Sequential checkouts that gradually drain stock. Last one should fail."""
        user = tu.create_user()
        user_id = user['user_id']
        tu.add_credit_to_user(user_id, 1000)

        item = tu.create_item(10)
        item_id = item['item_id']
        tu.add_stock(item_id, 5)

        # 5 orders each buying 1 item
        for i in range(5):
            order = tu.create_order(user_id)
            order_id = order['order_id']
            tu.add_item_to_order(order_id, item_id, 1)
            tu.checkout_order(order_id)
            self.assertTrue(tu.wait_for_order_paid(order_id, timeout=10), f"Order {i+1} should succeed")

        # stock should be 0 now
        self.assertEqual(tu.find_item(item_id)['stock'], 0)
        self.assertEqual(tu.find_user(user_id)['credit'], 950)

        # 6th order should fail
        order = tu.create_order(user_id)
        order_id = order['order_id']
        tu.add_item_to_order(order_id, item_id, 1)
        tu.checkout_order(order_id)

        paid = tu.wait_for_order_paid(order_id, timeout=5)
        self.assertFalse(paid)
        self.assertEqual(tu.find_item(item_id)['stock'], 0)
        self.assertEqual(tu.find_user(user_id)['credit'], 950)

if __name__ == '__main__':
    unittest.main()