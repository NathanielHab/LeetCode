class Solution(object):
    def reformatDate(self, date):
        """
        :type date: str
        :rtype: str
        """
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", 
                  "Sep", "Oct", "Nov", "Dec"]
        day, month, year = date.split(" ")
        day = day[:-2]
        if len(day) == 1:
            day = "0" + day
        month = str(months.index(month) + 1)
        if len(month) == 1:
            month = "0" + month
        ans = year + "-" + month + "-" + day
        return ans