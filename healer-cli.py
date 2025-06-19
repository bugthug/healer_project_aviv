# healer_cli.py
import click
import socket
import json
import os
from sqlalchemy.orm import joinedload
from database import (get_session_factory, setup_database, Avatar, InformationCopy,
                      Request, Session as DbSession, AvatarGroup, ICGroup, AvatarGroupMember, ICGroupMember)
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
def initdb():
    """Initializes the database (DESTRUCTIVE)."""
    setup_database()
    click.secho("Database initialized successfully.", fg='green')

@cli.command()
def ping():
    """Pings the daemon service."""
    response = send_command({"action": "ping"})
    if response.get('status') == 'success':
        click.secho(f"Success: {response.get('message', 'pong')}", fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

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
        color_map = {'RUNNING': 'green', 'COMPLETED': 'bright_blue', 'SCHEDULED': 'yellow', 'STOPPED': 'red', 'FAILED': 'bright_red'}
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

# --- View Group ---
@click.group(name='view')
def view_items():
    """View details of a specific entity."""
    pass

@view_items.command(name="avatar")
@click.argument('avatar_id', type=int)
@click.option('--save-photo', type=click.Path(dir_okay=False, writable=True), help="Path to save the avatar's photo.")
def view_avatar(avatar_id, save_photo):
    db = get_session_factory()()
    avatar = db.query(Avatar).get(avatar_id)
    if not avatar:
        click.secho(f"Error: Avatar ID {avatar_id} not found.", fg='red')
        db.close()
        return
    click.secho(f"--- Avatar: {avatar.name} (ID: {avatar.id}) ---", bold=True)
    click.echo(f"Created: {avatar.created_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if save_photo:
        try:
            with open(save_photo, 'wb') as f:
                f.write(avatar.photo_data)
            click.secho(f"Photo saved to: {os.path.abspath(save_photo)}", fg='green')
        except Exception as e:
            click.secho(f"Error saving photo: {e}", fg='red')
    click.secho("\n--- Info Data ---", bold=True)
    click.echo(avatar.info_data)
    click.secho("--- End Info ---", bold=True)
    db.close()

@view_items.command(name="running-on")
@click.argument('avatar_identifier')
def view_running_on(avatar_identifier):
    """View all running sessions on a specific avatar (by ID or name)."""
    command = {"action": "view_running_on", "data": {"avatar_identifier": avatar_identifier}}
    response = send_command(command)
    if response.get('status') == 'error':
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')
        return
    
    sessions = response.get('data', [])
    if not sessions:
        click.echo(f"No active sessions found for '{response.get('avatar_name', avatar_identifier)}'.")
        return
    
    click.secho(f"--- Active Sessions on {response.get('avatar_name')} (ID: {response.get('avatar_id')}) ---", bold=True)
    click.secho(f"{'ID':<5} {'Type':<18} {'Target':<55} {'Duration':<15}", bold=True)
    for s in sessions:
        duration_str = f"{s['duration_minutes']} min" if s['duration_minutes'] is not None else "Infinite"
        desc = s.get('target', 'N/A')
        click.echo(f"{s['session_id']:<5} {s['type']:<18} {desc[:52] + '...' if len(desc) > 52 else desc:<55} {duration_str:<15}")

cli.add_command(view_items)


# --- Edit Group ---
@click.group()
def edit():
    """Edit an existing entity. This may restart active sessions."""
    pass

@edit.command("avatar")
@click.argument('avatar_id', type=int)
@click.option('--name', help="New name for the avatar.")
@click.option('--photo', type=click.Path(exists=True, readable=True), help="New photo file for the avatar.")
@click.option('--info', type=click.Path(exists=True, readable=True), help="New info file for the avatar.")
def edit_avatar(avatar_id, name, photo, info):
    if not any([name, photo, info]):
        click.secho("Error: Must provide an option to edit.", fg='red'); return

    data = {"entity_type": "avatar", "id": avatar_id}
    if name: data['name'] = name
    if photo:
        with open(photo, 'rb') as f: data['photo_data_b64'] = json.dumps(list(f.read()))
    if info:
        with open(info, 'r', encoding='utf-8') as f: data['info_data'] = f.read()
    
    click.confirm("Editing an avatar may restart its active sessions. Continue?", abort=True)
    response = send_command({"action": "update_entity", "data": data})
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

cli.add_command(edit)

# --- Remove Group ---
@click.group()
def remove():
    """Remove an entity from the database."""
    pass

@remove.command(name="avatar")
@click.argument('avatar_id', type=int)
def remove_avatar(avatar_id):
    """Removes an avatar and ALL associated sessions and group memberships."""
    command = {"action": "remove_entity", "data": {"entity_type": "avatar", "id": avatar_id}}
    click.confirm(f"This will permanently delete Avatar ID {avatar_id} and stop all its sessions. Are you sure?", abort=True)
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
        if db.query(ICGroup).filter(ICGroup.name == name).first():
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


# --- Session Management ---
@click.group()
def session():
    """Commands to start and stop sessions."""
    pass

@session.command(name="start-ic")
@click.option('--avatar-id', type=int)
@click.option('--avatar-group', help="Name of the target avatar group.")
@click.option('--ic-id', type=int, required=True)
@click.option('--duration', type=int, help="Session duration in minutes.")
def start_ic_session(avatar_id, avatar_group, ic_id, duration):
    if not (avatar_id or avatar_group) or (avatar_id and avatar_group):
        click.secho("Error: Must specify --avatar-id or --avatar-group.", fg='red'); return
    command = {"action": "start_ic", "data": locals()}
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
    command = {"action": "start_group", "data": locals()}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='green')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

@session.command(name="stop")
@click.option('--session-id', required=True, type=int)
def stop_session(session_id):
    command = {"action": "stop_session", "data": {"session_id": session_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message'), fg='yellow')
    else:
        click.secho(f"Error: {response.get('message')}", fg='red')

cli.add_command(session)


if __name__ == '__main__':
    cli()
