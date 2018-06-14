def edits(word):
    '''
    This function is to return words that is an edit away from the original word.
    Please refer to http://norvig.com/spell-correct.html for more information.

    :param str word: error-prone keyword
    :return list[str]: list of candidate words
    '''
    alphabet = 'abcdefghijklmnopqrstuvwxyz'
    splits     = [(word[:i], word[i:]) for i in range(len(word) + 1)]
    deletes    = [a + b[1:] for a, b in splits if b]
    transposes = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b)>1]
    replaces   = [a + c + b[1:] for a, b in splits for c in alphabet if b]
    inserts    = [a + c + b     for a, b in splits for c in alphabet]
    return list(set(deletes + transposes + replaces + inserts))


def getCandidates(keyword, edit_distance, sc):
    '''
    Based on the acceptable tolerance (edit_distance) the list of candidates would increase if its more lenient.

    :param str keyword:
    :param int edit_distance:
    :param sparkContext sc:
    :return list[string]:
    '''
    candidates = [keyword]
    for lvl in range(edit_distance):
        new_candidates = []
        if lvl < 2:
            for candidate in candidates:
                new_candidates += edits(candidate)
            new_candidates = list(set(new_candidates))
        else:
            new_candidates = sc.parallelize(candidates).map(edits).reduce(lambda x, y: list(set(x + y)))

        candidates = new_candidates

    if edit_distance > 0:
        candidates.append(keyword)
    return candidates
