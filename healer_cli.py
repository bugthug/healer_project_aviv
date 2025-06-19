import click
import socket
import json
import os
import tempfile
import subprocess
import shutil
import time
import base64
from sqlalchemy.orm import joinedload
from database import (get_session_factory, Avatar, InformationCopy,
                      Request, Session as DbSession, AvatarGroup, ICGroup, AvatarGroupMember, ICGroupMember, RequestGroup, RequestGroupMember, Base)
from config import DAEMON_HOST, DAEMON_PORT

def send_command(command: dict):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((DAEMON_HOST, DAEMON_PORT))
            s.sendall(json.dumps(command).encode('utf-8'))
            response = s.recv(16384)
            return json.loads(response.decode('utf-8'))
    except ConnectionRefusedError:
        return {"status": "error", "message": f"Could not connect to daemon at {DAEMON_HOST}:{DAEMON_PORT}."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@click.group()
def cli():
    """Quantum Healer CLI."""
    pass

# --- Core Commands ---
@cli.command()
def ping():
    """Pings the daemon service."""
    try:
        response = send_command({"command": "ping"})
        if response["status"] == "success":
            click.secho("Daemon is running.", fg='green')
        else:
            click.secho(f"Daemon responded with: {response['message']}", fg='red')
    except Exception as e:
        click.secho(f"Error pinging daemon: {e}", fg='red')

# --- Add Group ---
@click.group()
def add():
    """Adds new entities to the database."""
    pass

@add.command(name="avatar")
@click.option('--name', required=True)
@click.option('--photo', type=click.Path(exists=True, readable=True), required=True)
@click.option('--info', type=click.Path(exists=True, readable=True), required=True)
def add_avatar(name, photo, info):
    db = get_session_factory()()
    try:
        if db.query(Avatar).filter_by(name=name).first():
            click.secho(f"Error: Avatar '{name}' already exists.", fg='red')
            return
        with open(photo, 'rb') as f_photo, open(info, 'r', encoding='utf-8') as f_info:
            new_avatar = Avatar(name=name, photo_data=f_photo.read(), info_data=f_info.read())
        db.add(new_avatar)
        db.commit()
        click.secho(f"Avatar '{name}' added with ID {new_avatar.id}.", fg='green')
    finally:
        db.close()

@add.command(name="ic")
@click.option('--name', required=True)
@click.option('--file', type=click.Path(exists=True, readable=True), required=True)
def add_ic(name, file):
    db = get_session_factory()()
    try:
        if db.query(InformationCopy).filter_by(name=name).first():
            click.secho(f"Error: IC '{name}' already exists.", fg='red')
            return
        with open(file, 'rb') as f:
            new_ic = InformationCopy(name=name, wav_data=f.read())
        db.add(new_ic)
        db.commit()
        click.secho(f"IC '{name}' added with ID {new_ic.id}.", fg='green')
    finally:
        db.close()

@add.command(name="request")
@click.option('--name', required=True)
@click.option('--file', type=click.Path(exists=True, readable=True), required=True)
def add_request(name, file):
    db = get_session_factory()()
    try:
        if db.query(Request).filter_by(name=name).first():
            click.secho(f"Error: Request '{name}' already exists.", fg='red')
            return
        with open(file, 'r', encoding='utf-8') as f:
            new_request = Request(name=name, request_data=f.read())
        db.add(new_request)
        db.commit()
        click.secho(f"Request '{name}' added with ID {new_request.id}.", fg='green')
    finally:
        db.close()

cli.add_command(add)

# --- List Group ---
@click.group(name='list')
def list_items():
    """Lists entities from the database."""
    pass

@list_items.command(name="avatars")
def list_avatars():
    db = get_session_factory()()
    avatars = db.query(Avatar).order_by(Avatar.id).all()
    if not avatars:
        click.echo("No avatars found.")
        return
    click.secho(f"{'ID':<5} {'Name':<30}", bold=True)
    for av in avatars:
        click.echo(f"{av.id:<5} {av.name:<30}")
    db.close()

@list_items.command(name="ics")
def list_ics():
    db = get_session_factory()()
    ics = db.query(InformationCopy).order_by(InformationCopy.id).all()
    if not ics:
        click.echo("No ICs found.")
        return
    click.secho(f"{'ID':<5} {'Name':<30}", bold=True)
    for ic in ics:
        click.echo(f"{ic.id:<5} {ic.name:<30}")
    db.close()

@list_items.command(name="requests")
def list_requests():
    db = get_session_factory()()
    requests = db.query(Request).order_by(Request.id).all()
    if not requests:
        click.echo("No requests found.")
        return
    click.secho(f"{'ID':<5} {'Name':<30}", bold=True)
    for r in requests:
        click.echo(f"{r.id:<5} {r.name:<30}")
    db.close()

@list_items.command(name="sessions")
@click.option('--limit', default=20, help="Number of recent sessions to show.")
def list_sessions(limit):
    db = get_session_factory()()
    sessions = db.query(DbSession).order_by(DbSession.id.desc()).limit(limit).all()
    if not sessions:
        click.echo("No sessions found.")
        return
    click.secho(f"{'ID':<5} {'Parent':<7} {'Type':<18} {'Description':<55} {'Status':<12}", bold=True)
    for s in sessions:
        color_map = {'RUNNING': 'green', 'COMPLETED': 'bright_blue', 'SCHEDULED': 'yellow', 'STOPPED': 'red', 'FAILED': 'bright_red', 'RESTARTED': 'blue'}
        color = color_map.get(s.status.value, 'white')
        desc = s.description or "N/A"
        click.secho(
            f"{s.id:<5} "
            f"{'#'+str(s.parent_session_id) if s.parent_session_id else '':<7} "
            f"{s.session_type.value:<18} "
            f"{desc[:52] + '...' if len(desc) > 52 else desc:<55} "
            f"{s.status.value:<12}",
            fg=color
        )
    db.close()

@list_items.command(name="groups-ic")
def list_ic_groups():
    db = get_session_factory()()
    groups = db.query(ICGroup).options(joinedload(ICGroup.members)).order_by(ICGroup.id).all()
    if not groups:
        click.echo("No IC groups found.")
        return
    click.secho(f"{'ID':<5} {'Name':<25} {'Members'}", bold=True)
    for group in groups:
        click.echo(f"{group.id:<5} {group.name:<25} {len(group.members)}")
    db.close()

@list_items.command(name="groups-avatar")
def list_avatar_groups():
    db = get_session_factory()()
    groups = db.query(AvatarGroup).options(joinedload(AvatarGroup.members)).order_by(AvatarGroup.id).all()
    if not groups:
        click.echo("No Avatar groups found.")
        return
    click.secho(f"{'ID':<5} {'Name':<25} {'Members'}", bold=True)
    for group in groups:
        click.echo(f"{group.id:<5} {group.name:<25} {len(group.members)}")
    db.close()

cli.add_command(list_items)

# --- Import/Export Commands ---
@cli.command()
@click.option('--output-file', '-o', default='healer_db_export.json', help='The file to export the database to.')
def export(output_file):
    """Exports the entire database to a JSON file."""
    db = get_session_factory()()
    data_to_export = {}

    # The order of tables is important for import
    # Start with tables that don't have foreign keys to others
    table_order = [
        'avatars', 'information_copies', 'requests', 'avatar_groups', 'ic_groups', 'request_groups',
        'avatar_group_members', 'ic_group_members', 'request_group_members', 'sessions'
    ]

    click.echo("Starting database export...")

    for table_name in table_order:
        if table_name in Base.metadata.tables:
            table = Base.metadata.tables[table_name]
            model = next(m for m in Base.registry.mappers if m.local_table == table).class_
            
            click.echo(f"Exporting table: {table_name}")
            
            records = db.query(model).all()
            data_list = []
            for record in records:
                record_dict = {}
                for column in table.columns:
                    value = getattr(record, column.name)
                    if isinstance(value, bytes):
                        record_dict[column.name] = base64.b64encode(value).decode('utf-8')
                    elif isinstance(value, (datetime.datetime, datetime.date)):
                        record_dict[column.name] = value.isoformat()
                    elif isinstance(value, enum.Enum):
                        record_dict[column.name] = value.value
                    else:
                        record_dict[column.name] = value
                data_list.append(record_dict)
            data_to_export[table_name] = data_list
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_export, f, indent=4, ensure_ascii=False)
        click.secho(f"Database successfully exported to {output_file}", fg='green')
    except IOError as e:
        click.secho(f"Error writing to file: {e}", fg='red')
    finally:
        db.close()

@cli.command()
@click.option('--input-file', '-i', type=click.Path(exists=True, readable=True), required=True, help='The JSON file to import the database from.')
def import_db(input_file):
    """Imports the database from a JSON file, overwriting existing data."""
    
    if not click.confirm(click.style("This is a destructive operation. It will wipe all current data. Do you want to continue?", fg='yellow', bold=True)):
        click.echo("Import cancelled.")
        return

    db = get_session_factory()()
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data_to_import = json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        click.secho(f"Error reading or parsing the import file: {e}", fg='red')
        db.close()
        return

    click.echo("Starting database import...")
    
    # Reverse order for deletion to respect foreign keys
    table_order = [
        'sessions', 'request_group_members', 'ic_group_members', 'avatar_group_members',
        'request_groups', 'ic_groups', 'avatar_groups', 'requests', 'information_copies', 'avatars'
    ]
    
    click.echo("Clearing existing data...")
    for table_name in table_order:
        if table_name in Base.metadata.tables:
            table = Base.metadata.tables[table_name]
            click.echo(f"Deleting from {table_name}")
            db.execute(table.delete())

    # Forward order for insertion
    table_order.reverse()
    
    try:
        for table_name in table_order:
            if table_name in data_to_import:
                table = Base.metadata.tables[table_name]
                model = next(m for m in Base.registry.mappers if m.local_table == table).class_
                records = data_to_import[table_name]
                
                click.echo(f"Importing data for {table_name}...")
                
                for record_dict in records:
                    for column in table.columns:
                        if column.name in record_dict and record_dict[column.name] is not None:
                            # Handle binary data
                            if isinstance(column.type, LargeBinary):
                                record_dict[column.name] = base64.b64decode(record_dict[column.name])
                            # Handle datetime
                            elif isinstance(column.type, (DateTime,)):
                                record_dict[column.name] = datetime.datetime.fromisoformat(record_dict[column.name])
                    
                    db.add(model(**record_dict))
        
        click.echo("Committing changes...")
        db.commit()
        click.secho("Database successfully imported.", fg='green')

    except Exception as e:
        db.rollback()
        click.secho(f"An error occurred during import: {e}", fg='red')
    finally:
        db.close()

# --- View Group ---
@click.group(name='view')
def view_items():
    """View details of a specific entity."""
    pass

@view_items.command(name="avatar")
@click.argument('avatar_id', type=int)
@click.option('--photo', is_flag=True, help="Attempt to open the avatar's photo.")
def view_avatar(avatar_id, photo):
    db = get_session_factory()()
    avatar = db.query(Avatar).get(avatar_id)
    db.close()
    if not avatar:
        click.secho(f"Error: Avatar ID {avatar_id} not found.", fg='red')
        return

    click.secho(f"--- Avatar: {avatar.name} (ID: {avatar.id}) ---", bold=True)
    click.echo(f"Created: {avatar.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    if photo:
        if not shutil.which("xdg-open"):
            click.secho("Error: 'xdg-open' command not found. Cannot open photo automatically.", fg='red')
        else:
            temp_path = ""
            try:
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=".jpg") as tmp_file:
                    tmp_file.write(avatar.photo_data)
                    temp_path = tmp_file.name
                
                click.echo(f"Attempting to open photo with the default application...")
                subprocess.run(['xdg-open', temp_path], check=True)
                time.sleep(2)
            except Exception as e:
                click.secho(f"Error opening photo: {e}", fg='red')
            finally:
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)

    click.secho("\n--- Info Data ---", bold=True)
    click.echo(avatar.info_data)
    click.secho("--- End Info ---", bold=True)

@view_items.command(name="request")
@click.argument('request_id', type=int)
def view_request(request_id):
    """Displays the details and content of a specific request."""
    db = get_session_factory()()
    req = db.query(Request).get(request_id)
    db.close()
    if not req:
        click.secho(f"Error: Request ID {request_id} not found.", fg='red')
        return

    click.secho(f"--- Request: {req.name} (ID: {req.id}) ---", bold=True)
    click.echo(f"Created: {req.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    click.secho("\n--- Request Content ---", bold=True)
    click.echo(req.request_data)
    click.secho("--- End Content ---", bold=True)

@view_items.command(name="running-on")
@click.argument('avatar_identifier')
def view_running_on(avatar_identifier):
    """View all running sessions on a specific avatar (by ID or name)."""
    command = {"action": "view_running_on", "data": {"avatar_identifier": avatar_identifier}}
    response = send_command(command)
    if response.get('status') == 'error':
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')
        return
    
    click.secho(f"--- Running Sessions on {response['avatar_name']} (ID: {response['avatar_id']}) ---", bold=True)
    sessions = response.get('data', [])
    if not sessions:
        click.echo("No running sessions found.")
    else:
        click.secho(f"{'ID':<8} {'Type':<18} {'Target/Description':<55} {'Duration (min)':<15}", bold=True)
        for s in sessions:
            click.echo(f"{s['session_id']:<8} {s['type']:<18} {s['target']:<55} {s.get('duration_minutes') or 'N/A'}")

cli.add_command(view_items)

# --- Edit Group ---
@click.group()
def edit():
    """Edits existing entities."""
    pass

@edit.command("avatar")
@click.argument('avatar_id', type=int)
@click.option('--name', help="New name for the avatar.")
@click.option('--photo', type=click.Path(exists=True, readable=True), help="New photo file for the avatar.")
@click.option('--info', type=click.Path(exists=True, readable=True), help="New info file for the avatar.")
def edit_avatar(avatar_id, name, photo, info):
    """Edits an avatar's details and restarts affected sessions."""
    if not any([name, photo, info]):
        click.secho("Error: At least one option (--name, --photo, --info) must be provided.", fg='red')
        return

    data = {"entity_type": "avatar", "id": avatar_id}
    if name:
        data['name'] = name
    if photo:
        with open(photo, 'rb') as f:
            data['photo_data_b64'] = json.dumps(list(f.read())) # Serialize bytes
    if info:
        with open(info, 'r', encoding='utf-8') as f:
            data['info_data'] = f.read()

    response = send_command({"action": "update_entity", "data": data})
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@edit.command("request")
@click.argument('request_id', type=int)
@click.option('--name', help="New name for the request.")
@click.option('--file', type=click.Path(exists=True, readable=True), help="New file for the request.")
def edit_request(request_id, name, file):
    """Updates a request's details."""
    if not name and not file:
        click.secho("Error: At least one option (--name or --file) must be provided.", fg='red')
        return
    data = {"entity_type": "request", "entity_id": request_id}
    if name: data['name'] = name
    if file:
        with open(file, 'r', encoding='utf-8') as f:
            data['content'] = f.read()
    response = send_command({"action": "update_entity", "data": data})
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

cli.add_command(edit)

# --- Remove Group ---
@click.group()
def remove():
    """Removes entities from the database."""
    pass

@remove.command(name="avatar")
@click.argument('avatar_id', type=int)
def remove_avatar(avatar_id):
    """Removes an avatar and stops related sessions."""
    command = {"action": "remove_entity", "data": {"entity_type": "avatar", "id": avatar_id}}
    click.confirm(f"Are you sure you want to delete avatar {avatar_id}? This will stop all related sessions.", abort=True)
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

cli.add_command(remove)

# --- Avatar Group Management ---
@click.group(name="group-avatar")
def group_avatar():
    """Manage Avatar Groups."""
    pass

@group_avatar.command(name="create")
@click.option('--name', required=True)
def create_avatar_group(name):
    db = get_session_factory()()
    try:
        if db.query(AvatarGroup).filter_by(name=name).first():
            click.secho(f"Error: Avatar group '{name}' already exists.", fg='red')
            return
        db.add(AvatarGroup(name=name))
        db.commit()
        click.secho(f"Avatar group '{name}' created.", fg='green')
    finally:
        db.close()

@group_avatar.command(name="delete")
@click.option('--name', required=True)
def delete_avatar_group(name):
    command = {"action": "remove_group", "data": {"group_type": "avatar", "group_name": name}}
    click.confirm(f"Are you sure you want to delete the Avatar group '{name}'? This will stop all related sessions.", abort=True)
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_avatar.command(name="add-member")
@click.option('--group-name', required=True)
@click.option('--avatar-id', required=True, type=int)
def add_avatar_to_group(group_name, avatar_id):
    command = {"action": "add_member_to_group", "data": {"group_type": "avatar", "group_name": group_name, "member_id": avatar_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_avatar.command(name="remove-member")
@click.option('--group-name', required=True)
@click.option('--avatar-id', required=True, type=int)
def remove_avatar_from_group(group_name, avatar_id):
    command = {"action": "remove_member_from_group", "data": {"group_type": "avatar", "group_name": group_name, "member_id": avatar_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_avatar.command(name="show")
@click.option('--name', required=True)
def show_avatar_group(name):
    db = get_session_factory()()
    try:
        group = db.query(AvatarGroup).filter(AvatarGroup.name == name).options(joinedload(AvatarGroup.members).joinedload(AvatarGroupMember.avatar)).first()
        if not group:
            click.secho(f"Error: Avatar group '{name}' not found.", fg='red')
            return
        click.secho(f"--- Avatar Group: {group.name} (ID: {group.id}) ---", bold=True)
        if not group.members:
            click.echo("This group is empty.")
        else:
            click.secho(f"{'ID':<5} {'Name'}", bold=True)
            for member in group.members:
                click.echo(f"{member.avatar.id:<5} {member.avatar.name}")
    finally:
        db.close()

cli.add_command(group_avatar)

# --- IC Group Management ---
@click.group(name="group-ic")
def group_ic():
    """Manage IC Groups."""
    pass

@group_ic.command(name="create")
@click.option('--name', required=True)
def create_ic_group(name):
    db = get_session_factory()()
    try:
        if db.query(ICGroup).filter_by(name=name).first():
            click.secho(f"Error: IC group '{name}' already exists.", fg='red')
            return
        db.add(ICGroup(name=name))
        db.commit()
        click.secho(f"IC group '{name}' created.", fg='green')
    finally:
        db.close()

@group_ic.command(name="delete")
@click.option('--name', required=True)
def delete_ic_group(name):
    command = {"action": "remove_group", "data": {"group_type": "ic", "group_name": name}}
    click.confirm(f"Are you sure you want to delete the IC group '{name}'? This will stop all related sessions.", abort=True)
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_ic.command(name="add-member")
@click.option('--group-name', required=True)
@click.option('--ic-id', required=True, type=int)
def add_ic_to_group(group_name, ic_id):
    command = {"action": "add_member_to_group", "data": {"group_type": "ic", "group_name": group_name, "member_id": ic_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_ic.command(name="remove-member")
@click.option('--group-name', required=True)
@click.option('--ic-id', required=True, type=int)
def remove_ic_from_group(group_name, ic_id):
    command = {"action": "remove_member_from_group", "data": {"group_type": "ic", "group_name": group_name, "member_id": ic_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_ic.command(name="show")
@click.option('--name', required=True)
def show_ic_group(name):
    db = get_session_factory()()
    try:
        group = db.query(ICGroup).filter(ICGroup.name == name).options(joinedload(ICGroup.members).joinedload(ICGroupMember.ic)).first()
        if not group:
            click.secho(f"Error: IC group '{name}' not found.", fg='red')
            return
        click.secho(f"--- IC Group: {group.name} (ID: {group.id}) ---", bold=True)
        if not group.members:
            click.echo("This group is empty.")
        else:
            click.secho(f"{'ID':<5} {'Name'}", bold=True)
            for member in group.members:
                click.echo(f"{member.ic.id:<5} {member.ic.name}")
    finally:
        db.close()

cli.add_command(group_ic)

# --- Request Group Management ---
@click.group(name="group-request")
def group_request():
    """Manage Request Groups."""
    pass

@group_request.command(name="create")
@click.option('--name', required=True)
def create_request_group(name):
    db = get_session_factory()()
    try:
        if db.query(RequestGroup).filter_by(name=name).first():
            click.secho(f"Error: Request group '{name}' already exists.", fg='red')
            return
        db.add(RequestGroup(name=name))
        db.commit()
        click.secho(f"Request group '{name}' created.", fg='green')
    finally:
        db.close()

@group_request.command(name="delete")
@click.option('--name', required=True)
def delete_request_group(name):
    command = {"action": "remove_group", "data": {"group_type": "request", "group_name": name}}
    click.confirm(f"Are you sure you want to delete the Request group '{name}'? This will stop all related sessions.", abort=True)
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_request.command(name="add-member")
@click.option('--group-name', required=True)
@click.option('--request-id', required=True, type=int)
def add_request_to_group(group_name, request_id):
    command = {"action": "add_member_to_group", "data": {"group_type": "request", "group_name": group_name, "member_id": request_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_request.command(name="remove-member")
@click.option('--group-name', required=True)
@click.option('--request-id', required=True, type=int)
def remove_request_from_group(group_name, request_id):
    command = {"action": "remove_member_from_group", "data": {"group_type": "request", "group_name": group_name, "member_id": request_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@group_request.command(name="show")
@click.option('--name', required=True)
def show_request_group(name):
    db = get_session_factory()()
    try:
        # Eagerly load members and the request associated with each member
        group = db.query(RequestGroup).filter(RequestGroup.name == name).options(
            joinedload(RequestGroup.members).joinedload(RequestGroupMember.request)
        ).first()

        if not group:
            click.secho(f"Error: Request group '{name}' not found.", fg='red')
            return
            
        click.secho(f"--- Request Group: {group.name} (ID: {group.id}) ---", bold=True)
        if not group.members:
            click.echo("This group is empty.")
        else:
            click.secho(f"{'ID':<5} {'Name'}", bold=True)
            for member in group.members:
                click.echo(f"{member.request.id:<5} {member.request.name}")
    finally:
        db.close()

cli.add_command(group_request)

# --- Session Management ---
@click.group()
def session():
    """Commands for managing sessions."""
    pass

@session.command(name="start-ic")
@click.option('--avatar-id', type=int)
@click.option('--avatar-group', help="Name of the target avatar group.")
@click.option('--ic-id', type=int, required=True)
@click.option('--duration', type=int, help="Session duration in minutes.")
def start_ic_session(avatar_id, avatar_group, ic_id, duration):
    """Starts an IC session on an avatar or avatar group."""
    if not avatar_id and not avatar_group:
        click.secho("Error: Must specify either --avatar-id or --avatar-group.", fg='red')
        return
    command = {"action": "start_ic", "data": {
        "avatar_id": avatar_id, "avatar_group": avatar_group, "ic_id": ic_id, "duration": duration
    }}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@session.command(name="start-request")
@click.option('--avatar-id', type=int)
@click.option('--avatar-group', help="Name of the target avatar group.")
@click.option('--request-id', type=int)
@click.option('--request-group', help="Name of the target request group.")
@click.option('--duration', type=int, help="Session duration in minutes.")
def start_request_session(avatar_id, avatar_group, request_id, request_group, duration):
    """Starts a request session on an avatar or avatar group."""
    if not avatar_id and not avatar_group:
        click.secho("Error: Must specify either --avatar-id or --avatar-group.", fg='red')
        return
    if not request_id and not request_group:
        click.secho("Error: Must specify either --request-id or --request-group.", fg='red')
        return
    command = {"action": "start_request", "data": {
        "avatar_id": avatar_id, "avatar_group": avatar_group,
        "request_id": request_id, "request_group": request_group,
        "duration": duration
    }}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@session.command(name="start-link")
@click.option('--source-id', type=int, required=True, help="The source avatar for the link.")
@click.option('--dest-id', type=int, help="A single destination avatar.")
@click.option('--dest-group', help="Name of the destination avatar group.")
@click.option('--duration', type=int, help="Session duration in minutes.")
def start_link_session(source_id, dest_id, dest_group, duration):
    """Starts a link session from a source avatar to a destination avatar or group."""
    if not dest_id and not dest_group:
        click.secho("Error: Must specify either --dest-id or --dest-group.", fg='red')
        return
    command = {"action": "start_link", "data": {
        "source_id": source_id, "dest_id": dest_id, "dest_group": dest_group, "duration": duration
    }}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@session.command(name="start-group")
@click.option('--avatar-group', required=True, help="Name of the target avatar group.")
@click.option('--ic-group', required=True, help="Name of the IC group.")
@click.option('--duration', type=int, help="Session duration in minutes.")
def start_group_session(avatar_group, ic_group, duration):
    """Starts a group session between an avatar group and an IC group."""
    command = {"action": "start_group", "data": {
        "avatar_group": avatar_group, "ic_group": ic_group, "duration": duration
    }}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@session.command(name="stop")
@click.option('--session-id', required=True, type=int)
def stop_session(session_id):
    """Stops a specific running session."""
    response = send_command({"action": "stop_session", "data": {"session_id": session_id}})
    if response['status'] == 'success':
        click.secho(f"Session {session_id} stopped successfully.", fg='green')
    else:
        click.secho(f"Error: {response['message']}", fg='red')

@session.command(name="fail")
@click.option('--avatar-id', type=int, help="ID of the target avatar.")
@click.option('--avatar-group', help="Name of the target avatar group.")
def fail_sessions(avatar_id, avatar_group):
    """Fails all running sessions for a specific avatar or avatar group."""
    if not avatar_id and not avatar_group:
        click.secho("Error: You must provide either --avatar-id or --avatar-group.", fg='red')
        return
    if avatar_id and avatar_group:
        click.secho("Error: You can only provide one of --avatar-id or --avatar-group.", fg='red')
        return

    command = {"action": "fail_sessions_on_target", "data": {}}
    if avatar_id:
        command['data']['avatar_id'] = avatar_id
    if avatar_group:
        command['data']['avatar_group'] = avatar_group

    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@session.command(name="fail-all-running")
def fail_all_running():
    """Stops all currently running sessions and marks them as FAILED."""
    response = send_command({"action": "fail_all_running"})
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@session.command(name="redo-all-failed")
def redo_all_failed():
    """Restarts all sessions currently in a FAILED state."""
    response = send_command({"action": "redo_failed", "data": {}})
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

cli.add_command(session)

# --- Main Entry Point ---
if __name__ == '__main__':
    cli()