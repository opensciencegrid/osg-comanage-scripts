#!/usr/bin/env python3

import os
import sys
import json
import getopt
import comanage_utils as utils
import comanage_person_schema_utils as schema_utils

SCRIPT = os.path.basename(__file__)
ENDPOINT = "https://registry.cilogon.org/registry/"
OSG_CO_ID = 7
CMS_GROUP_ID = 4622
CMS_COU_ID = 1785
LDAP_TARGET_ID = 9

_usage = f"""\
usage: [PASS=...] {SCRIPT} [OPTIONS]
"""

def usage(msg=None):
    if msg:
        print(msg + "\n", file=sys.stderr)

    print(_usage, file=sys.stderr)
    sys.exit()

class Options:
    endpoint = ENDPOINT
    user = "co_7.project_script"
    osg_co_id = OSG_CO_ID
    authstr = None
    input_file = None
    mapping_file = None
    ssh_key_authenticator = 5
    unix_cluster_id = 10
    provisioning_target = LDAP_TARGET_ID


options = Options()

def parse_options(args):
    try:
        ops, args = getopt.getopt(args, 'u:c:d:f:e:i:m:h')
    except getopt.GetoptError:
        usage()

    if args:
        usage("Extra arguments: %s" % repr(args))

    passfd = None
    passfile = None

    for op, arg in ops:
        if op == '-h': usage()
        if op == '-u': options.user       = arg
        if op == '-c': options.osg_co_id  = int(arg)
        if op == '-d': passfd             = int(arg)
        if op == '-f': passfile           = arg
        if op == '-e': options.endpoint   = arg
        if op == '-i': options.input_file = arg
        if op == '-m': options.mapping_file = arg

    try:
        user, passwd = utils.getpw(options.user, passfd, passfile)
        options.authstr = utils.mkauthstr(user, passwd)
    except PermissionError:
        usage("PASS required")


def read_data_dump():
    data_json = []
    with open(options.input_file, 'r', encoding='utf-8') as input_file:
        data_json = json.load(input_file)
        for entry in range(len(data_json)):
            for key_index in range(len(data_json[entry]["public_keys"])):
                key = data_json[entry]["public_keys"][key_index]
                key_sections = str(key).split()
                if len(key_sections) >= 2:
                    data_json[entry]["public_keys"][key_index] = {"type" : key_sections[0], "pkey" : key_sections[1]}
                if len(key_sections) >= 3:
                    data_json[entry]["public_keys"][key_index].update({"authenticator" : key_sections[2]})
    with open(options.mapping_file, 'r', encoding='utf-8') as mapping_file:
        mapping_json = json.load(mapping_file)
    return data_json


def build_co_person_record(entry):
    record = {}
    record.update({"CoPerson" : schema_utils.co_person_schema(options.osg_co_id, status="A")})
    
    names = []
    names.append(schema_utils.name_split(entry["name"]))
    record.update({"Name" : names})

    identifiers = []

    # CMS Username
    identifiers.append(schema_utils.co_person_identifier(entry["username"], "cmsuser", status="A"))
    # CMS UID
    identifiers.append(schema_utils.co_person_identifier(entry["uid"], "cmsuid", status="A"))
    #globus id
    identifiers.append(schema_utils.co_person_identifier(entry["globus_id"], "cmsglobusid", status="A"))
    #cilogon id
    if not entry["cilogon_id"] is None:
        identifiers.append(schema_utils.co_person_identifier(entry["cilogon_id"], "oidcsub", status="A"))
    else:
        print(f"Warning: user {entry['username']} lacks a cilogon id.")

    record.update({"Identifier" : identifiers })

    group_memberships = []
    group_memberships.append(schema_utils.co_person_group_member(CMS_GROUP_ID))

    # Group Memberships
    record.update({"CoGroupMember" : group_memberships })

    roles = []

    roles.append(schema_utils.co_person_role(CMS_COU_ID, "CMS User", "member", 1))
    record.update({"CoPersonRole" : roles })

    emails = []

    emails.append(schema_utils.co_person_email_address(entry["email"]))
    record.update({"EmailAddress" : emails})

    org_ids = []

    org_ids.append(schema_utils.co_person_org_id(options.osg_co_id, names, entry["institution"], id_list=identifiers))
    record.update({"OrgIdentity" : org_ids})

    ssh_keys = []

    for ssh_key in entry["public_keys"]:
        key_type = ssh_key["type"]
        public_key = ssh_key["pkey"]
        comment = ""
        if "authenticator" in dict(ssh_key):
            comment = f"SSH Key for {ssh_key['authenticator']}"
        auth_id = options.ssh_key_authenticator
        ssh_keys.append(schema_utils.co_person_sshkey(type=key_type, skey=public_key, comment=comment, auth_id=auth_id))
    record.update({"SshKey" : ssh_keys})

    return record


def create_unix_cluster_group(co_person_record):
    identifiers_list = co_person_record["Identifier"]
    username = next((item["identifier"] for item in identifiers_list if item["type"] == "osguser"))
    uid = next((item["identifier"] for item in identifiers_list if item["type"] == "uid"))
    description = f"Unix Cluster Group for {username}"
    result = utils.create_co_group(username, description, options.osg_co_id, options.endpoint, options.authstr)
    ucg = None
    if not (result is None) and ("ResponseType" in result) and (result["ResponseType"] == "NewObject"):
        group_id = result["Id"]
        utils.add_identifier_to_group(group_id, "osggid", uid, options.endpoint, options.authstr)
        utils.add_identifier_to_group(group_id, "osggroup", username, options.endpoint, options.authstr)
        ucg = utils.add_unix_cluster_group(group_id, options.unix_cluster_id, options.endpoint, options.authstr)
        utils.provision_group(group_id, options.provisioning_target, options.endpoint, options.authstr)
    #TODO throw catch on new group creation
    if not (ucg is None) and ("ResponseType" in ucg) and (ucg["ResponseType"] == "NewObject"):
        return(result["Id"])
    else:
        raise ValueError(f"Failed to create CO Group for Unix Cluster Group, results were: {result} and {ucg}")


def add_unix_cluster_account(co_person_record):
    identifiers_list = co_person_record["Identifier"]
    names_list = co_person_record["Name"]
    username = next((item["identifier"]  for item in identifiers_list if item["type"] == "osguser"))
    uid = next((item["identifier"]  for item in identifiers_list if item["type"] == "uid"))
    name_id = next((item for item in names_list if item["primary_name"] == True))
    name = schema_utils.name_unsplit(name_id)
    default_group_id = -1
    default_group_id = create_unix_cluster_group(co_person_record)
    ucg_membership = schema_utils.co_person_group_member(default_group_id)
    if "CoGroupMember" in co_person_record:
        co_person_record["CoGroupMember"].append(ucg_membership)
    else:
        co_person_record.update({"CoGroupMember" : [ucg_membership]})
    if default_group_id != -1:
        uca = schema_utils.co_person_unix_cluster_acc(options.unix_cluster_id, username, uid, name, default_group_id)
        if "UnixClusterAccount" in co_person_record:
            co_person_record["UnixClusterAccount"].append(uca)
        else:
            co_person_record.update({"UnixClusterAccount" : [uca]})
    return co_person_record, default_group_id


def main(args):
    parse_options(args)

    co_person_records = dict()

    data_dump_json = read_data_dump()
    for entry in data_dump_json:
        co_person_records.update({entry["username"] : build_co_person_record(entry)})

    usernames = list(co_person_records.keys())

    for user in usernames:
        co_person_data = None
        results_create = None

        try:
            try:
                #If the CO Person record exists, stop creating/modifying (TODO: switch to modifying existing user rather than trying to create)
                if utils.core_api_co_person_read(user, options.osg_co_id, options.endpoint, options.authstr):
                    continue
            except utils.HTTPRequestError as e:
                # If the record *doesn't* exist, pass and make it. Else, some other error happened on our read, like 403 or 500 and we'll try again on another run.
                if e.code == 404:
                    pass
                else:
                    break
            print(f"CREATING RECORDS FOR USER: {user}")
            results_create = utils.core_api_co_person_create(data=co_person_records[user], coid=options.osg_co_id,     endpoint=options.endpoint, authstr=options.authstr)

            co_person_data = utils.core_api_co_person_read(user, options.osg_co_id, options.endpoint, options.authstr)

            co_person_data, gid = add_unix_cluster_account(co_person_data)

            utils.core_api_co_person_update(user, options.osg_co_id, co_person_data, options.endpoint, options.authstr)

            utils.provision_group(gid, options.provisioning_target, options.endpoint, options.authstr)
        except Exception as e:
            print(f"\tException for user {user}.")
            print(f"\t{e}")
            if results_create:
                print(f"\t{results_create}")
                if co_person_data:
                    print(f"\t{co_person_data}")

    # Read in dump to build / update users from
        # select which field of the dump co-responds to the identifier we'll use to index the corresponding CO Person
        # mapping file from dump attributes to COmanage object types, so we know what each dump attribute should become

    # Loop over entries in dump 
        # try a CO Person Read based on specified field
            # if Read succeeds, we're doing an update
            # else if we fail due to the record not existing, we're doing a create
            # else, something went wrong and we should log about it

    # Needs:
        # Create base CO Person record with at least enough information to read them at a later date
        # Read CO Person via index identifier
        # Write CO Person with specified full schema
        # "matching identifier/key/name already exists in CO Person entry?" function
        # 
        # Data -> Write schema functions, with argument for existing field to merge data in from, for 
            # Base CO Person
            # Identifiers
            # Email
            # Name
            # SSHKey
            # Org ID
            # CO Person Role
            # UNIX Cluster Account
            # GroupMembership (?)

    # Schema:
        # CoPerson
            # {
            # 'meta': {'id': , 'created': , 'modified': , 'co_person_id': , 'revision': , 'deleted': , 'actor_identifier': },
            # 'co_id': <8 for test / 7 for prod>,
            # 'status': 'A',
            # 'timezone': None,
            # 'date_of_birth': None
            # }
        # Identifier
            # {
            # 
            # }
        # CoGroupMember 
            # Array of {
                # 'meta': {'id': 9119, 'source_org_identity_id': None, 'created': '2025-05-14 21:47:15', 'modified': '2025-05-14 21:47:15', 'co_group_member_id': None, 'revision': 0, 'deleted': False, 'actor_identifier': 'co_8.william_test'},
                # 'co_group_id': 101, 
                # 'member': True, 
                # 'owner': False, 
                # 'valid_from': None, 
                # 'valid_through': None, 
                # 'co_group_nesting_id': None 
                # }
        # Name
            # Array of {
            # 'honorific': None, 
            # 'given': 'TEST_PERSON', 
            # 'middle': None, 
            # 'family': 'Wanson', 
            # 'suffix': None, 
            # 'type': 'official', 
            # 'language': None, 
            # 'primary_name': True
            # }
        # SSHKEY
            # Array of {
            # 'type': '<ssh key type>',
            # 'skey': '<public_key>',
            # 'ssh_key_authenticator_id': 5
            # }
            # NOTE: the "5" is from https://registry-test.cilogon.org/registry/ssh_key_authenticator/ssh_key_authenticators/edit/5


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except Exception as e:
        sys.exit(e)