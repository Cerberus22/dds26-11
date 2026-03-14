import unittest

import utils as tu


class TestMicroservices(unittest.TestCase):

    def test_setUpClass(cls):
        #Seed fixed test data via the service HTTP APIs.

        #Uses batch_init endpoints which store entries at keys "0", "1", etc.,
        #giving us known fixed IDs without needing direct Redis access.

        #After this runs:
        #  cls.user_id  = "0"  (credit = 15)
        #  cls.item_id1 = "0"  (stock = 1, price = 5)
        #  cls.item_id2 = "1"  (stock = 2, price = 5)
        #  cls.order_id  — created via API, user_id=cls.user_id, items added manually
        
        import requests

        # user "0" with credit=15
        requests.post("http://127.0.0.1:8000/payment/batch_init/1/15")
        cls.user_id = "0"

        # item "0" stock=1 price=5, item "1" stock=2 price=5
        requests.post("http://127.0.0.1:8000/stock/batch_init/2/1/5")
        cls.item_id1 = "0"  # stock=1
        cls.item_id2 = "1"  # stock=2

        # bump item "1" to stock=2 (batch_init sets all to the same starting value,
        # so add 1 more to item_id2 to make it stock=2 while item_id1 stays at 1)
        tu.add_stock(cls.item_id2, 1)

        # create an order for user "0" and add both items
        order = tu.create_order(cls.user_id)
        cls.order_id = order["order_id"]
        print(f"Created order with ID {cls.order_id}")
        tu.add_item_to_order(cls.order_id, cls.item_id1, 1)
        tu.add_item_to_order(cls.order_id, cls.item_id2, 1)

    # run the first test 
    # then crash one of the services (docker stop <service_name>)
    # then run the second test to see that the system is resilient to the failure
    # then restart the service and check its logs (docker logs <service_name>) to see if it recovered
    # note that you have to copy paste the order_id from the test output to the test code
    #def test_order_checkout_happy_path(self):
    #    checkout_response = tu.checkout_order('58becb91-6603-4ca5-b195-6a7d9953e4fe').status_code
    #    self.assertTrue(tu.status_code_is_failure(checkout_response))
    
if __name__ == '__main__':
    unittest.main()
