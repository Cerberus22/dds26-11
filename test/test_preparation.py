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
        
        user = tu.create_user()
        cls.user_id = user["user_id"]
        tu.add_credit_to_user(cls.user_id, 15)

        # Create items via API
        cls.item_id1 = tu.create_item(5)["item_id"]
        tu.add_stock(cls.item_id1, 1)

        cls.item_id2 = tu.create_item(5)["item_id"]
        tu.add_stock(cls.item_id2, 2)

        # Create order via API
        order = tu.create_order(cls.user_id)
        cls.order_id = order["order_id"]
        print(f"order_id={cls.order_id}")

        tu.add_item_to_order(cls.order_id, cls.item_id1, 1)
        tu.add_item_to_order(cls.order_id, cls.item_id2, 1)

    # run the first test 
    # then crash one of the services (docker stop <service_name>)
    # then run the second test to see that the system is resilient to the failure
    # then restart the service and check its logs (docker logs <service_name>) to see if it recovered
    # note that you have to copy paste the order_id from the test output to the test code
    #def test_order_checkout_happy_path(self):
    #    checkout_response = tu.checkout_order('7ae0a8f0-98de-4c5f-ac08-2d529e94060f@169cd58b-4959-4f3d-8f94-f34ce1c781b1').status_code
    #    self.assertTrue(tu.status_code_is_failure(checkout_response))
    
if __name__ == '__main__':
    unittest.main()
