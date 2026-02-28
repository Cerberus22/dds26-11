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

        # verify state after, both must have changed atomically
        self.assertEqual(tu.find_item(item_id)['stock'], 3)
        self.assertEqual(tu.find_user(user_id)['credit'], 80)
        self.assertTrue(tu.find_order(order_id)['paid'])


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
        self.assertTrue(tu.status_code_is_failure(response.status_code))

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
        self.assertTrue(tu.status_code_is_failure(response.status_code))

        # stock must be fully restored, this is the key 2PC guarantee
        self.assertEqual(tu.find_item(item_id)['stock'], 5)
        self.assertEqual(tu.find_user(user_id)['credit'], 5)
        self.assertFalse(tu.find_order(order_id)['paid'])


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

        # simulate recovery re-sending commit by calling commit endpoints directly
        # this should be a no-op, not double deduct
        # check that stock and credit are correct after a second checkout attempt
        second_response = tu.checkout_order(order_id)
        # stock and credit must not change again
        self.assertEqual(tu.find_item(item_id)['stock'], 4)
        self.assertEqual(tu.find_user(user_id)['credit'], 90)

if __name__ == '__main__':
    unittest.main()