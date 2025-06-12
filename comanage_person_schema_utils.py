#!/usr/bin/env python3

CO_PERSON = {"co_id": None, "timezone": None, "dateofbirth": None, "status": None}

IDENTIFIER = {
    "identifier": None,
    "type": None,
    "login": False,
    "status": None,
}

NAME = {
    "honorific": None,
    "given": "None",
    "middle": None,
    "family": "None",
    "suffix": None,
    "type": "official",
    "language": None,
    "primary_name": True,
}


GROUP = {
    "co_group_id": None,
    "member": True,
    "owner": False,
}


COU = {
    "co_id": None,
    "name": None,
    "description": None,
    "parent_id": None,
}


ORG_IDENTITY = {
    "co_id": None,
    "title": None,
    # Organization for OrgID
    "o": None,
    # Department for OrgID
    "ou": None,
    "valid_from": None,
    "valid_through": None,
    "status": "",
    "affiliation": None,
    "date_of_birth": None,
    "Address": [],
    "AdHocAttribute": [],
    "EmailAddress": [],
    "Identifier": [],
    "Name": [],
    "TelephoneNumber": [],
    "Url": [],
}


EMAIL_ADDRESS = {
    "mail": None,
    "type": None,
    "verified": None,
}


UNIX_CLUSTER_ACCOUNT = {
    "sync_mode": "F",
    "status": None,
    "username": None,
    "uid": None,
    "gecos": None,
    "login_shell": "/bin/bash",
    "home_directory": None,
    "primary_co_group_id": None,
    "valid_from": None,
    "valid_through": None,
    "unix_cluster_id": None,
}


SSHKEY = {
    "type": None,
    "skey": None,
    "comment": "",
    "ssh_key_authenticator_id": None,
}


def co_person_schema(co_id, timezone=None, dob=None, status="Active"):
    person_data = CO_PERSON.copy()
    person_data["co_id"] = co_id
    person_data["timezone"] = timezone
    person_data["dateofbirth"] = dob
    person_data["status"] = status
    return person_data


def co_person_identifier(identifier, type, login=False, status="Active"):
    identifier_data = IDENTIFIER.copy()
    identifier_data["identifier"] = identifier
    identifier_data["type"] = type
    identifier_data["login"] = login
    identifier_data["status"] = status
    return identifier_data


def co_person_name(given, family=None, middle=None, type="official", primary=False):
    name_data = NAME.copy()
    name_data["given"] = given
    name_data["family"] = family
    name_data["middle"] = middle
    name_data["type"] = type
    name_data["primary_name"] = primary
    return name_data


def name_split(whole_name):
    name_sections = str(whole_name).split()
    parts_count = len(name_sections)
    if parts_count == 1:
        return co_person_name(given=whole_name)
    elif parts_count == 2:
        return co_person_name(given=name_sections[0], family=name_sections[1])
    else:
        return co_person_name(given=name_sections[0], family=name_sections[parts_count-1], middle=" ".join(name_sections[1:parts_count-1]))


def name_unsplit(name_id):
    if name_id["given"] is None:
        return ""
    elif name_id["family"] is None:
        return name_id["given"]
    elif name_id["middle"] is None:
        return f'{name_id["given"]} {name_id["family"]}'
    else:
        return f'{name_id["given"]} {name_id["middle"]} {name_id["family"]}'



def co_person_group_member(group_id, member=True, owner=False):
    group_member = GROUP.copy()
    group_member["co_group_id"] = group_id
    group_member["member"] = member
    group_member["owner"] = owner
    return group_member


def co_person_org_id(
    osg_co_id, name, organization="", department="", title="", affiliation="member", id_list=[]
):
    #org_id = {"co_id" : osg_co_id}
    org_id = ORG_IDENTITY.copy()
    org_id["co_id"] = osg_co_id
    org_id["title"] = title
    org_id["o"] = organization
    org_id["ou"] = department
    org_id["affiliation"] = affiliation
    org_id["Identifier"] = id_list
    org_id["Name"] = name
    return org_id


def co_person_email_address(mail, type="delivery", verified=False):
    email = EMAIL_ADDRESS.copy()
    email["mail"] = mail
    email["type"] = type
    email["verified"] = verified
    return email


def co_person_unix_cluster_acc(unix_cluster_id, username, uid, name, group_id, status="A"):
    uca = UNIX_CLUSTER_ACCOUNT.copy()
    uca["unix_cluster_id"] = unix_cluster_id
    uca["username"] = username
    uca["uid"] = uid
    uca["status"] = status
    uca["gecos"] = name
    uca["home_directory"] = f"/home/{username}"
    uca["primary_co_group_id"] = group_id
    return uca


def co_person_sshkey(type, skey, comment, auth_id):
    sshkey_data = SSHKEY.copy()
    sshkey_data["type"] = type
    sshkey_data["skey"] = skey
    sshkey_data["comment"] = comment
    sshkey_data["ssh_key_authenticator_id"] = auth_id
    return sshkey_data


# def merge_schema(base_data, new_data, type):
#    temp = base_data
#    for field in type.keys():
#        for entry in new_data[field]:
#            if "meta" in entry:
#
#
#    return temp
