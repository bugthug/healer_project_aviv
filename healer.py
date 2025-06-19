# healer_cli.py
import click
import socket
import json
import os
from sqlalchemy.exc import IntegrityError
from database import (get_session_factory, setup_database, Avatar, InformationCopy,
                      Request, Session as DbSession, SessionStatus, SessionType,
                      ICGroup, ICGroupMember, AvatarGroup, AvatarGroupMember)
from config import DAEMON_HOST, DAEMON_PORT

Session_Factory = get_session_factory()

def send_command(command: dict):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((DAEMON_HOST, DAEMON_PORT))
            s.sendall(json.dumps(command).encode('utf-8'))
            response = s.recv(8192)
            return json.loads(response.decode('utf-8'))
    except ConnectionRefusedError:
        return {"status": "error", "message": f"Could not connect to the daemon at {DAEMON_HOST}:{DAEMON_PORT}. Is it running?"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@click.group()
def cli():
    """Quantum Healer CLI - Manage avatars, ICs, groups, and healing sessions."""
    pass

@cli.command()
def initdb():
    """Initializes the PostgreSQL database (DESTRUCTIVE)."""
    setup_database()
    click.echo("Database initialized successfully.")

@cli.command()
def ping():
    """Check if the daemon service is running."""
    response = send_command({"action": "ping"})
    if response.get('status') == 'success':
        click.secho(f"Success: Daemon is alive and responded: {response['message']}", fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

@click.group()
def add():
    """Add new Avatars, ICs, or Requests to the database."""
    pass

@add.command(name="avatar")
@click.option('--name', required=True)
@click.option('--photo', type=click.Path(exists=True, readable=True), required=True)
@click.option('--info', type=click.Path(exists=True, readable=True), required=True)
def add_avatar(name, photo, info):
    db = Session_Factory()
    try:
        if db.query(Avatar).filter(Avatar.name == name).first():
            click.secho(f"Error: Avatar with name '{name}' already exists.", fg='red')
            return
        with open(photo, 'rb') as f_photo, open(info, 'r', encoding='utf-8') as f_info:
            photo_bytes = f_photo.read()
            info_str = f_info.read()
        new_avatar = Avatar(name=name, photo_data=photo_bytes, info_data=info_str)
        db.add(new_avatar)
        db.commit()
        click.secho(f"Avatar '{name}' added successfully with ID {new_avatar.id}.", fg='green')
    except Exception as e:
        db.rollback()
        click.secho(f"An error occurred: {e}", fg='red')
    finally:
        db.close()

@add.command(name="ic")
@click.option('--name', required=True)
@click.option('--file', type=click.Path(exists=True, readable=True), required=True)
def add_ic(name, file):
    db = Session_Factory()
    try:
        if db.query(InformationCopy).filter(InformationCopy.name == name).first():
            click.secho(f"Error: IC with name '{name}' already exists.", fg='red')
            return
        with open(file, 'rb') as f:
            wav_bytes = f.read()
        new_ic = InformationCopy(name=name, wav_data=wav_bytes)
        db.add(new_ic)
        db.commit()
        click.secho(f"IC '{name}' added successfully with ID {new_ic.id}.", fg='green')
    except Exception as e:
        db.rollback()
        click.secho(f"An error occurred: {e}", fg='red')
    finally:
        db.close()

@add.command(name="request")
@click.option('--name', required=True)
@click.option('--file', type=click.Path(exists=True, readable=True), required=True)
def add_request(name, file):
    db = Session_Factory()
    try:
        if db.query(Request).filter(Request.name == name).first():
            click.secho(f"Error: Request with name '{name}' already exists.", fg='red')
            return
        with open(file, 'r', encoding='utf-8') as f:
            request_data = f.read()
        new_request = Request(name=name, request_data=request_data)
        db.add(new_request)
        db.commit()
        click.secho(f"Request '{name}' added successfully with ID {new_request.id}.", fg='green')
    except Exception as e:
        db.rollback()
        click.secho(f"An error occurred: {e}", fg='red')
    finally:
        db.close()

cli.add_command(add)

@click.group(name='list')
def list_items():
    """List Avatars, ICs, Requests, Groups, or Sessions."""
    pass

@list_items.command()
def avatars():
    db = Session_Factory()
    try:
        all_avatars = db.query(Avatar).all()
        if not all_avatars: click.echo("No avatars found.")
        else:
            click.secho(f"{'ID':<5} {'Name':<30} {'Created (UTC)':<20}", bold=True)
            for av in all_avatars:
                click.echo(f"{av.id:<5} {av.name:<30} {av.created_at.strftime('%Y-%m-%d %H:%M')}")
    finally:
        db.close()

@list_items.command()
def ics():
    db = Session_Factory()
    try:
        all_ics = db.query(InformationCopy).all()
        if not all_ics: click.echo("No ICs found.")
        else:
            click.secho(f"{'ID':<5} {'Name':<30} {'Created (UTC)':<20}", bold=True)
            for ic in all_ics:
                click.echo(f"{ic.id:<5} {ic.name:<30} {ic.created_at.strftime('%Y-%m-%d %H:%M')}")
    finally:
        db.close()
        
@list_items.command()
def requests():
    db = Session_Factory()
    try:
        all_requests = db.query(Request).all()
        if not all_requests:
            click.echo("No requests found.")
            return
        click.secho(f"{'ID':<5} {'Name':<30} {'Created (UTC)':<20}", bold=True)
        for r in all_requests:
            click.echo(f"{r.id:<5} {r.name:<30} {r.created_at.strftime('%Y-%m-%d %H:%M')}")
    finally:
        db.close()

@list_items.command(name="groups-ic")
def list_ic_groups():
    db = Session_Factory()
    try:
        all_groups = db.query(ICGroup).all()
        if not all_groups: click.echo("No IC groups found.")
        else:
            click.secho(f"{'ID':<5} {'Name':<25} {'Members'}", bold=True)
            for group in all_groups:
                click.echo(f"{group.id:<5} {group.name:<25} {len(group.members)}")
    finally:
        db.close()

@list_items.command(name="groups-avatar")
def list_avatar_groups():
    db = Session_Factory()
    try:
        all_groups = db.query(AvatarGroup).all()
        if not all_groups: click.echo("No Avatar groups found.")
        else:
            click.secho(f"{'ID':<5} {'Name':<25} {'Members'}", bold=True)
            for group in all_groups:
                click.echo(f"{group.id:<5} {group.name:<25} {len(group.members)}")
    finally:
        db.close()

@list_items.command()
@click.option('--limit', default=30)
def sessions(limit):
    db = Session_Factory()
    try:
        all_sessions = db.query(DbSession).order_by(DbSession.id.desc()).limit(limit).all()
        if not all_sessions: click.echo("No sessions found."); return

        click.secho(f"{'ID':<5} {'Parent':<7} {'Type':<18} {'Description':<55} {'Status':<12}", bold=True)
        for s in all_sessions:
            color = {'RUNNING': 'green', 'COMPLETED': 'bright_blue', 'SCHEDULED': 'yellow', 'STOPPED': 'red', 'FAILED': 'bright_red'}.get(s.status.value, 'white')
            status_str = s.status.value
            session_type_str = s.session_type.value
            parent_str = f"#{s.parent_session_id}" if s.parent_session_id else " "
            
            desc = s.description if s.description else "N/A"
            if len(desc) > 52:
                desc = desc[:52] + "..."

            click.secho(f"{s.id:<5} {parent_str:<7} {session_type_str:<18} {desc:<55} {status_str:<12}", fg=color)
    finally:
        db.close()

cli.add_command(list_items)

@click.group()
def view():
    """View details of a specific Avatar, IC, or running sessions."""
    pass

@view.command(name="avatar")
@click.argument('avatar_id', type=int)
@click.option('--save-photo', type=click.Path(), help="Path to save the avatar's photo.")
def view_avatar(avatar_id, save_photo):
    db = Session_Factory()
    try:
        avatar = db.query(Avatar).filter(Avatar.id == avatar_id).first()
        if not avatar: click.secho(f"Error: Avatar with ID {avatar_id} not found.", fg='red'); return
        
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

    finally:
        db.close()

@view.command(name="running-on")
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
        click.echo(f"No active sessions found for '{avatar_identifier}'.")
        return

    click.secho(f"--- Active Sessions on {response.get('avatar_name')} (ID: {response.get('avatar_id')}) ---", bold=True)
    click.secho(f"{'ID':<5} {'Type':<18} {'Target':<40} {'Duration':<15}", bold=True)
    for s in sessions:
        duration_str = f"{s['duration_minutes']} min" if s['duration_minutes'] is not None else "Infinite"
        click.echo(f"{s['session_id']:<5} {s['type']:<18} {s['target']:<40} {duration_str:<15}")

cli.add_command(view)

@click.group()
def edit():
    """Edit an existing Avatar, IC, or Request. This will restart any active sessions using them."""
    pass

@edit.command("avatar")
@click.argument('avatar_id', type=int)
@click.option('--photo', type=click.Path(exists=True, readable=True))
@click.option('--info', type=click.Path(exists=True, readable=True))
def edit_avatar(avatar_id, photo, info):
    if not photo and not info:
        click.secho("Error: Must provide --photo or --info.", fg='red'); return

    data = {"entity_type": "avatar", "id": avatar_id}
    if photo:
        with open(photo, 'rb') as f: data['photo_data_b64'] = json.dumps(list(f.read()))
    if info:
        with open(info, 'r', encoding='utf-8') as f: data['info_data'] = f.read()
    
    click.confirm("Editing an avatar will restart ALL active sessions using it. Continue?", abort=True)
    response = send_command({"action": "update_entity", "data": data})
    if response.get('status') == 'success':
        click.secho(response.get('message', 'Avatar updated successfully.'), fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

cli.add_command(edit)

@click.group(name="group-ic")
def group_ic():
    """Manage IC Groups."""
    pass

@group_ic.command(name="create")
@click.option('--name', required=True)
def create_ic_group(name):
    db = Session_Factory()
    try:
        if db.query(ICGroup).filter(ICGroup.name == name).first():
            click.secho(f"Error: IC group '{name}' already exists.", fg='red'); return
        db.add(ICGroup(name=name))
        db.commit()
        click.secho(f"Successfully created IC group '{name}'.", fg='green')
    finally:
        db.close()

@group_ic.command(name="delete")
@click.option('--name', required=True)
def delete_ic_group(name):
    db = Session_Factory()
    try:
        group = db.query(ICGroup).filter(ICGroup.name == name).first()
        if not group: click.secho(f"Error: IC group '{name}' not found.", fg='red'); return
        click.confirm(f"Are you sure you want to delete the IC group '{name}'?", abort=True)
        db.delete(group)
        db.commit()
        click.secho(f"Successfully deleted IC group '{name}'.", fg='yellow')
    finally:
        db.close()

@group_ic.command(name="add-member")
@click.option('--group-name', required=True)
@click.option('--ic-id', required=True, type=int)
def add_ic_member(group_name, ic_id):
    command = {"action": "add_member_to_group", "data": {"group_type": "ic", "group_name": group_name, "member_id": ic_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message', 'Member added.'), fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

@group_ic.command(name="remove-member")
@click.option('--group-name', required=True)
@click.option('--ic-id', required=True, type=int)
def remove_ic_member(group_name, ic_id):
    command = {"action": "remove_member_from_group", "data": {"group_type": "ic", "group_name": group_name, "member_id": ic_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message', 'Member removed.'), fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

@group_ic.command(name="show")
@click.option('--name', required=True)
def show_ic_group(name):
    db = Session_Factory()
    try:
        group = db.query(ICGroup).filter(ICGroup.name == name).first()
        if not group: click.secho(f"Error: IC group '{name}' not found.", fg='red'); return
        click.secho(f"--- IC Group: {group.name} (ID: {group.id}) ---", bold=True)
        if not group.members: click.echo("This group has no members.")
        else:
            click.secho("Members:", bold=True)
            for member in group.members:
                click.echo(f"  - IC ID: {member.ic.id:<5} Name: {member.ic.name}")
    finally:
        db.close()

cli.add_command(group_ic)

@click.group(name="group-avatar")
def group_avatar():
    """Manage Avatar Groups."""
    pass

@group_avatar.command(name="create")
@click.option('--name', required=True)
def create_avatar_group(name):
    db = Session_Factory()
    try:
        if db.query(AvatarGroup).filter(AvatarGroup.name == name).first():
            click.secho(f"Error: Avatar group '{name}' already exists.", fg='red'); return
        db.add(AvatarGroup(name=name))
        db.commit()
        click.secho(f"Successfully created avatar group '{name}'.", fg='green')
    finally:
        db.close()

@group_avatar.command(name="delete")
@click.option('--name', required=True)
def delete_avatar_group(name):
    db = Session_Factory()
    try:
        group = db.query(AvatarGroup).filter(AvatarGroup.name == name).first()
        if not group: click.secho(f"Error: Avatar group '{name}' not found.", fg='red'); return
        click.confirm(f"Are you sure you want to delete avatar group '{name}'?", abort=True)
        db.delete(group)
        db.commit()
        click.secho(f"Successfully deleted avatar group '{name}'.", fg='yellow')
    finally:
        db.close()

@group_avatar.command(name="add-member")
@click.option('--group-name', required=True)
@click.option('--avatar-id', required=True, type=int)
def add_avatar_member(group_name, avatar_id):
    command = {"action": "add_member_to_group", "data": {"group_type": "avatar", "group_name": group_name, "member_id": avatar_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message', 'Member added.'), fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

@group_avatar.command(name="remove-member")
@click.option('--group-name', required=True)
@click.option('--avatar-id', required=True, type=int)
def remove_avatar_member(group_name, avatar_id):
    command = {"action": "remove_member_from_group", "data": {"group_type": "avatar", "group_name": group_name, "member_id": avatar_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message', 'Member removed.'), fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

@group_avatar.command(name="show")
@click.option('--name', required=True)
def show_avatar_group(name):
    db = Session_Factory()
    try:
        group = db.query(AvatarGroup).filter(AvatarGroup.name == name).first()
        if not group: click.secho(f"Error: Avatar group '{name}' not found.", fg='red'); return
        click.secho(f"--- Avatar Group: {group.name} (ID: {group.id}) ---", bold=True)
        if not group.members: click.echo("This group has no members.")
        else:
            click.secho("Members:", bold=True)
            for member in group.members:
                click.echo(f"  - Avatar ID: {member.avatar.id:<5} Name: {member.avatar.name}")
    finally:
        db.close()

cli.add_command(group_avatar)

@click.group()
def remove():
    """Remove Avatars, ICs, or Requests from the database."""
    pass

@remove.command(name="avatar")
@click.option('--id', 'avatar_id', type=int, required=True, help="The ID of the avatar to remove.")
def remove_avatar(avatar_id):
    """Removes an avatar and ALL associated sessions and group memberships."""
    click.confirm(f"This will remove Avatar {avatar_id} and stop all related sessions. Continue?", abort=True)
    command = {"action": "remove_entity", "data": {"entity_type": "avatar", "id": avatar_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message', 'Avatar removed successfully.'), fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

cli.add_command(remove)

@click.group()
def session():
    """Commands to start, stop, and manage sessions."""
    pass

@session.command(name="start-ic")
@click.option('--avatar-id', type=int)
@click.option('--avatar-group', help="Name of the target avatar group.")
@click.option('--ic-id', type=int, required=True)
@click.option('--duration', type=int, required=False, help="Omit for infinite.")
def start_ic_session(avatar_id, avatar_group, ic_id, duration):
    if not (avatar_id or avatar_group) or (avatar_id and avatar_group):
        click.secho("Error: Must specify exactly one of --avatar-id or --avatar-group.", fg='red'); return
    command = {"action": "start_ic", "data": {"avatar_id": avatar_id, "avatar_group": avatar_group, "ic_id": ic_id, "duration": duration}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message', 'Sessions started.'), fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

@session.command(name="start-group")
@click.option('--avatar-group', required=True, help="Name of the target avatar group.")
@click.option('--ic-group', required=True, help="Name of the IC Group.")
@click.option('--duration', type=int, required=False)
def start_group_session(avatar_group, ic_group, duration):
    command = {"action": "start_group", "data": {"avatar_group": avatar_group, "ic_group": ic_group, "duration": duration}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response.get('message', 'Sessions started.'), fg='green')
    else:
        click.secho(f"Error: {response.get('message', 'Unknown error.')}", fg='red')

@session.command()
@click.option('--session-id', type=int, required=True)
def stop(session_id):
    """Stops a session. If it's a group session, stops all child sessions."""
    command = {"action": "stop_session", "data": {"session_id": session_id}}
    response = send_command(command)
    if response.get('status') == 'success':
        click.secho(response['message'], fg='yellow')
    else:
        click.secho(f"Error: {response['message']}", fg='red')
    
cli.add_command(session)

if __name__ == '__main__':
    cli()
