class ParseConstants:
    # ASCII codes
    LPAREN = ord('(')
    RPAREN = ord(')')
    WHITESPACE = ord(' ')
    TAB = ord('\t')
    A = ord('A')
    N = ord('N')
    D = ord('D')
    O = ord('O')
    R = ord('R')
    S = ord('S')
    K = ord('K')
    I = ord('I')
    P = ord('P')
    W = ord('W')
    H = ord('H')
    L = ord('L')
    E = ord('E')
    BACKSLASH = ord('\\')
    SINGLE_QUOTE = ord('\'')
    COMMA = ord(',')
    EQUAL_TO = ord('=')
    COLON = ord(':')
    ZERO = ord('0')
    NINE = ord('9')
    MINUS_SIGN = ord('-')

    # Byte arrays
    SKIP_ARRAY = b'SKIP'
    WHILE_ARRAY = b'WHILE'
    OR_ARRAY = b'OR'
    AND_ARRAY = b'AND'
    LESS_THAN_ARRAY = b'<'
    LESS_THAN_OR_EQUAL_TO_ARRAY = b'<='
    GREATER_THAN_ARRAY = b'>'
    GREATER_THAN_OR_EQUAL_TO_ARRAY = b'>='
    EQUAL_TO_ARRAY = b'='
    NOT_EQUAL_TO_ARRAY = b'!='
    LPAREN_ARRAY = b'('

    # ByteBuffer equivalents in Python (simply using byte arrays)
    SKIP_BUFFER = SKIP_ARRAY
    WHILE_BUFFER = WHILE_ARRAY
    OR_BUFFER = OR_ARRAY
    AND_BUFFER = AND_ARRAY
    LESS_THAN_BUFFER = LESS_THAN_ARRAY
    LESS_THAN_OR_EQUAL_TO_BUFFER = LESS_THAN_OR_EQUAL_TO_ARRAY
    GREATER_THAN_BUFFER = GREATER_THAN_ARRAY
    GREATER_THAN_OR_EQUAL_TO_BUFFER = GREATER_THAN_OR_EQUAL_TO_ARRAY
    EQUAL_TO_BUFFER = EQUAL_TO_ARRAY
    NOT_EQUAL_TO_BUFFER = NOT_EQUAL_TO_ARRAY
    LPAREN_BUFFER = LPAREN_ARRAY

    # String constants
    FILTER_PACKAGE = "org.apache.hadoop.hbase.filter"

    # Byte arrays for types
    binaryType = b'binary'
    binaryPrefixType = b'binaryprefix'
    regexStringType = b'regexstring'
    regexStringNoCaseType = b'regexstringnocase'
    substringType = b'substring'
