"""
Client for the Tagfiler REST API.
"""
import urllib
import json

from globusonline.catalog import rest_client

DEFAULT_BASE_URL = "https://localhost/tagfiler"


class TagfilerClient(rest_client.GoauthRestClient):
    """
    Note: all helper methods return the response object followed by the
    body, parsed from json if possible. The return values listed below in the
    docstrings just describe the body.

    On failure the method will raise RestClientError, and the body will
    typically be plain text (need to fix this in tagfielr).

    TODO: default limit in tagefiler is 25; all getters need to be updated
    with a limit parameter.
    """
    def __init__(self, goauth_token, base_url=DEFAULT_BASE_URL, max_attempts=1,
                 parse_json=True, log_requests=False):
        super(TagfilerClient, self).__init__(goauth_token, base_url,
                                             max_attempts, parse_json,
                                             log_requests)

    def _get_tags_format(self, tag_dict):
        """Convert a dictionary of tags into the tagfiler format.

        The tagfiler format is a semicolon separated list of name=value
        strings, with comma separated values used to specify multivalued
        tags. Tag names and values are percent encoded, which is needed to
        avoid conflicting with delimiters and operators as well as URL
        unsafe characters."""
        tag_array = []
        for k, v in tag_dict.iteritems():
            k = urlquote(k)
            if isinstance(v, list):
                v = ",".join(urlquote(e) for e in v)
            else:
                v = urlquote(v)
            tag_array.append("%s=%s" % (k, v))
        return ";".join(tag_array)

    def create_catalog(self, catalog_dict=None, **kw):
        """Create a catalog with the given name and addional attributes
        specified in a dictionary and/or keyword parameters.

        @return: dictionary of catalog properties (most notably 'id')
        """
        if catalog_dict is None:
            catalog_dict = kw
        else:
            catalog_dict.update(kw)
        body = json.dumps(catalog_dict)
        return self._request("POST", "/catalog", body)

    # Not supported by Tagfiler yet.. will be soon
    def update_catalog(self, catalog_id, **kw):
        """Update a catalog with the given name and addional attributes
        specified in keyword parameters.

        @return: dictionary of catalog properties
        """
        pass
        #body = json.dumps(kw)
        #print body
        #return self._request("PUT", "/catalog/%s"
        #                               % urlquote(catalog_id), body)

    def delete_catalog(self, catalog_id):
        """Delete the specified catalog."""
        return self._request("DELETE", "/catalog/%s"
                                       % urlquote(catalog_id))

    def get_catalogs(self):
        """Get a list of all catalogs.

        @return: list of catalog dictionaries
        """
        return self._request("GET", "/catalog")

    def get_catalog(self, catalog_id):
        """Get the specified catalog."""
        return self._request("GET", "/catalog/%s"
                                       % urlquote(catalog_id))

    def create_subject(self, catalog_id, tags=None, **kwtags):
        """Create a new subject in the specified catalog, optionally
        pre-populated with tags.

        @param catalog_id: catalog to create subject in
        @param tags: dictionary mapping tag names to tag values.
                     Multi-valued tags can be specified using a list as the
                     value.
        @param kwtags: tags with names that are legal Python identifiers
                       can be specified as keyword params. Overrides anything
                       in tags param if both are specified and contain the
                       same key.

        @return: dictionary of catalog attributes (most notably 'id')
        """
        if tags is None:
            tags = {}
        tags.update(kwtags)
        data = self._get_tags_format(tags)
        return self._request("POST", "/catalog/%s/subject/%s"
                                     % (urlquote(catalog_id), data))

    def delete_subject(self, catalog_id, subject_id):
        """Delete the specified subject in the specified catalog."""
        return self._request("DELETE", "/catalog/%s/subject/id=%s"
                                       % (urlquote(catalog_id),
                                          urlquote(subject_id)))

    def delete_subjects_by_tag(self, catalog_id, tag_name, tag_value):
        """Delete subjects in the specified catalog with the specified tag.

        If @tag_name is unique, this will delete at most one subject,
        otherwise it could delete many subjects."""
        return self._request("DELETE", "/catalog/%s/subject/%s=%s"
                                       % (urlquote(catalog_id),
                                          urlquote(tag_name),
                                          urlquote(tag_value)))

    def get_subjects_by_query_raw(self, catalog_id, query, limit=100):
        """Get subjects in the specified catalog using a raw tagfiler query
        language (already formatted and escaped).

        @param query: A selector statement followed by a projection statement
                      in parentheses. The selector is a semicolon separated
                      list of selectors of the form tagnameXtagvalue, where X
                      is an operator such as equals sign, or simply a bare
                      tagname to check for subjects that have a certain tag.
                      Names and values must be utf8 encoded and url escaped.
                      The projection statment determines what tags to return
                      for each of the matching subjects. It can include bare
                      tagnames, or tagname=value to return only tags that have
                      the given value (especially useful for multi-valued
                      tags). Names and values must be utf8 encoded and url
                      escaped.
        """
        return self._request("GET", "/catalog/%s/subject/%s?limit=%d"
                                    % (urlquote(catalog_id), query, limit))

    def get_subjects_by_query(self, catalog_id, selector_list,
                              projection_list, orderby_list=None, limit=100):
        """High level method for querying subjects which builds the tagfiler
        query from Python data structures.

        @param selector: List of selectors, which can have one of three forms:
                         (1) a single tagname which will match any subject
                         having that tag, (2) a tuple of (tagname, operator)
                         for unary operators, or (3) a triple of (tagname,
                         operator, tagvalues). For (3) the values can be a
                         single value, or a list of values.
        @param projection: List of tagnames and pairs of (tagname, tagvalue).
                           Specifies which tags to return for the matching
                           subjects.
        @param orderby_list: list of tag names to order the query
                             TODO: does this need to be subset of projection?
        @param limit: Maximum number of subjects to return. To page through
                      results in groups of size N, pass N+1 for the limit
                      to determine if another page is available, and use
                      a selector on the orderby_list elements to get
                      the next page. Integer offset based paging is not
                      supported because it is expensive.
        """
        sl = []
        pl = []
        for s in selector_list:
            if (not isinstance(s, (tuple, list))) or len(s) == 1:
                s = (s, UnaryOp.TAGGED)

            if len(s) == 2:
                tagname, op = s
                if op in _Binary:
                    raise ValueError("Binary operator '%s' requires second "
                                     "argument" % op)
                elif op not in _Unary:
                    raise ValueError("Unknown operator '%s'" % op)
                sl.append("%s%s" % (urlquote(tagname), op))
            elif len(s) == 3:
                tagname, op, value = s
                if op in _Unary:
                    raise ValueError("Unary operator '%s' does not support a"
                                     "second argument" % op)
                elif op not in _Binary:
                    raise ValueError("Unknown operator '%s'" % op)
                else:
                    if not isinstance(value, (tuple, list)):
                        value = (value,)
                    value = ",".join(urlquote(v) for v in value)
                    sl.append("%s%s%s" % (urlquote(tagname), op, value))
            else:
                raise ValueError("Selector expression must contain one, two "
                                +"or three values")
        for p in projection_list:
            if not isinstance(p, (tuple, list)):
                p = (p,)

            if len(p) == 1:
                pl.append(urlquote(p[0]))
            elif len(p) == 2:
                pl.append("%s=%s" % (urlquote(p[0]), urlquote(p[1])))
            else:
                raise ValueError("Projection expression must contain one or "
                                 "more values")

        query = "%s(%s)" % (";".join(sl), ";".join(pl))

        if orderby_list:
            query += ",".join(orderby_list)

        return self.get_subjects_by_query_raw(catalog_id, query, limit)

    # Karl is going to remove the requirement for specifying a subject
    # predicate here
    def create_subjects(self, catalog_id, subjects, unique_tag):
        """Create multiple subjects at the same time prepopulated withn any
        specified tags.

        Response is 204 No Content. To manipulate the newly created subjects
        or determine their ids, the unique tags from the request must be used.

        @param catalog_id: id of catalog to create subjects in
        @param subjects: list of dictionaries containing tags for each subject.
                         Multi-valued tags can be specified with a list as the
                         value.
        @param unique_tag: name of a tag that uniquely identifies each
                           subject being added. Must be present in each
                           subject dictionary.
        """
        # set of all tags from each subject
        tags = set(urlquote(tagname)
                   for subject in subjects for tagname in subject)
        unique_tag = urlquote(unique_tag)
        tags.remove(unique_tag)
        return self._request("PUT", "/catalog/%s/subject/%s(%s)"
                                    % (urlquote(catalog_id),
                                       unique_tag, ";".join(tags)),
                             body=json.dumps(subjects))

    def create_tagdef(self, catalog_id, tag_name, tag_type, multi_value=False,
                      read_policy="anonymous", write_policy="anonymous"):
        """Create a new tagdef in the specified catalog.

        @param catalog_id: id of catalog to add tagdef to
        @param tag_name: string name of tag to define
        @param tag_type: string name of tag type
        @param multi_value: if true, tag can have multiple values
        @param read_policy: comma separated list of users or groups who should
                            be able to read tags with this name
        @param write_policy: comma separated list of users or groups who should
                             be able to write tags with this name
        """
        data = { "dbtype": safestr(tag_type),
                 "multivalue": safestr(multi_value),
                 "readpolicy" : safestr(read_policy),
                 "writepolicy": safestr(write_policy) }

        return self._request("PUT", "/catalog/%s/tagdef/%s?%s"
                                    % (urlquote(catalog_id),
                                       urlquote(tag_name),
                                       urllib.urlencode(data)))

    def get_tagdef(self, catalog_id, tag_name):
        """Get tagdef in the specified catalog."""
        return self._request("GET", "/catalog/%s/tagdef/%s"
                                    % (urlquote(catalog_id),
                                       urlquote(tag_name)))

    def delete_tagdef(self, catalog_id, tag_name):
        """Deleted the specified tagdef from the given catalog."""
        return self._request("DELETE", "/catalog/%s/tagdef/%s"
                                       % (urlquote(catalog_id),
                                          urlquote(tag_name)))

    def create_tags(self, catalog_id, subject_id, tags):
        """Create one or more tags (single or multivalued) and associate them
        with the specified subject.

        @param catalog_id: id of catalog to create tags in
        @param subject_id: id of subject to associate tags with
        @param tags: dictionary mapping tag names to tag values. Multi-valued
                     tags can be specified with a list as the tag value.
        """
        data = self._get_tags_format(tags)
        return self._request("PUT", "/catalog/%s/tags/id=%s(%s)"
                                    % (urlquote(catalog_id),
                                       urlquote(subject_id), data))

    def delete_tag_by_query_raw(self, catalog_id, subject_id, tag_query):
        """Delete the tags from the specified subject,
        using an already urlencoded tag selector query."""
        return self._request("DELETE", "/catalog/%s/tags/id=%s(%s)"
                                       % (urlquote(catalog_id),
                                          urlquote(subject_id),
                                          tag_query))

    def delete_tag(self, catalog_id, subject_id, tag_name, tag_value=None):
        """Delete the given tag and/or value from the specified subject."""
        if tag_value is not None:
            query = "%s=%s" % (urlquote(tag_name), urlquote(tag_value))
        else:
            query = urlquote(tag_name)
        return self.delete_tag_by_query_raw(catalog_id, subject_id, query)

    # not sure how useful this method really is?
    def get_tag_by_query(self, catalog_id, query):
        """Get the list of tags that match the specified tag query.

        @param catalog_id: id of catalog to query for tags
        @param query: semicolon delimited
        """
        return self._request("GET", "/catalog/%s/tags/%s"
                                    % (urlquote(catalog_id), query))


def urlquote(x):
    """Quote a str, unicode, or value coercable to str, for safe insertion
    in a URL.

    Uses utf8 encoding for unicode. Also note that the tagfiler
    delimiters comma, semicolon, and equals sign will be percent
    encoded, so this is effective to use quoting individual query
    elements but should never be used on the full query.

    >>> urlquote("n;a,m e=")
    'n%3Ba%2Cm%20e%3D'
    >>> urlquote(100)
    '100'
    >>> urlquote(True)
    'True'
    >>> urlquote(False)
    'False'

    Unicode, small a with macron:
    >>> urlquote(u'\u0101')
    '%C4%81'

    utf8 already encoded:
    >>> urlquote("\xc4\x81")
    '%C4%81'
    """
    return urllib.quote(safestr(x), "")

def urlunquote(x):
    return urllib.unquote(x)

def safestr(x):
    """Convert x to str, encoding in utf8 if needed.

    Can be used to pre-process data passed to urlencode."""
    assert x is not None
    if isinstance(x, unicode):
        return x.encode("utf8")
    elif not isinstance(x, str):
        return str(x)
    return x


class DictObject(dict):
    """Mutable dict where keys can be accessed as instance variables."""
    def __getattr__(self, name):
        return self[name]


"""Enum of binary tagfiler operators."""
BinaryOp = DictObject(
    EQUAL="=",
    NOT_EQUAL="!=",
    GT=":gt:",
    GEQ=":geq:",
    LT=":lt:",
    LEQ=":leq:",
    FULLTEXT=":word:",
    NOT_FULLTEXT=":!word:",
    LIKE=":like:",
    SIMTO=":simto:",
    REGEXP=":regexp:",
    NOT_REGEXP=":!regexp:",
    REGEXP_CASE_INSENSITIVE=":ciregexp:",
    NOT_REGEXP_CASE_INSENSITIVE=":!ciregexp:",
)
_Binary = set(BinaryOp.values())

"""Enum of unary tagfiler operators."""
UnaryOp = DictObject(
    ABSENT=":absent:",
    TAGGED=""
)
_Unary = set(UnaryOp.values())

Op = DictObject()
Op.update(BinaryOp)
Op.update(UnaryOp)


if __name__ == "__main__":
    # For testing with ipython
    import sys
    if len(sys.argv) < 3:
        print "Usage: %s goauth_token [base_url]" % sys.argv[0]
        sys.exit(1)
    goauth_token = sys.argv[2]
    if len(sys.argv) > 3:
        base_url = sys.argv[3]
    else:
        base_url = DEFAULT_BASE_URL
    client = TagfilerClient(goauth_token, base_url)
