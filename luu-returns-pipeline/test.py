try:
    def worker(user_detail_recipie):

        #messy_name = user_detail_recipie["name"]
        #messy_email = user_detail_recipie["email"]

        clean_name = user_detail_recipie["name"].strip().title()
        clean_email = user_detail_recipie["email"].strip().lower()

        user_clean_list = {
        "user_id" : user_detail_recipie["user_id"],
        "name" : clean_name,
        "email": clean_email,
        "status": user_detail_recipie["status"]
        }
        return user_clean_list



    raw_api_data = [
        {"user_id": 101, "name": "  aLice smITH ", "email": "ALICE@company.com  ", "status": "active"},
        {"user_id": 102, "name": "BOB JONES", "email": "  bob.jones@Company.com", "status": "inactive"},
        {"user_id": 103, "name": "   cHaRLie bRown", "email": "charlie@COMPANY.com", "status": "active"}
        ]

    fresh_string = []

    for test in raw_api_data:
        if test["status"] == "active":
            cleaned = worker(test)
            fresh_string.append(cleaned)
    print(fresh_string)

except Exception as e:
    print(f"Error: {e}")
