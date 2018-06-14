from functions import procDiff


def getUDF_filterKeywords(candidates, udf, BooleanType):
    return udf(lambda string: any([candidate in string.lower() for candidate in candidates]), BooleanType())


def getUDF_procDiff(udf, StringType):
    return udf(lambda cookTime, prepTime: procDiff(cookTime, prepTime), StringType())
