def procDiff(cookTime, prepTime):

    '''
    The method is the parse the string to number of hours and number of minutes, convert them to minutes and categorize
     them accordingly.

    :param str cookTime:
    :param str prepTime:
    :return str:
    '''

    def extractHourMinutes(pt):
        hour = 0
        minute = 0

        remTime = pt[2:].split("H")
        if len(remTime) > 1:
            hour = int(remTime[0])
            remTime = [remTime[1]]
        remTime = remTime[0].split("M")
        if len(remTime) > 1:
            minute = int(remTime[0])

        return hour, minute

    if cookTime != "" and prepTime != "":
        ct_hour, ct_min = extractHourMinutes(cookTime)
        pt_hour, pt_min = extractHourMinutes(prepTime)

        fin_min = (ct_hour + pt_hour) * 60 + ct_min + pt_min

        if fin_min > 60:
            return "Hard"
        elif fin_min <= 60 and fin_min >= 30:
            return "Medium"
        elif fin_min < 30:
            return "Easy"
    else:
        return "Unknown"


