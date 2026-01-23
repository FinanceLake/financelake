from flask import Flask, render_template, request, jsonify, send_from_directory, abort
import os
import pathlib
import time
import subprocess
import shlex

app = Flask(__name__)

# Directory containing logs (relative to repo root)
LOG_DIR = os.path.join(os.getcwd(), 'log')


def safe_list_logs():
    if not os.path.isdir(LOG_DIR):
        return []
    files = []
    for name in sorted(os.listdir(LOG_DIR)):
        path = os.path.join(LOG_DIR, name)
        if os.path.isfile(path):
            stat = os.stat(path)
            files.append({
                'name': name,
                'size': stat.st_size,
                'mtime': int(stat.st_mtime)
            })
    return files


def tail_lines(path, lines=200):
    # Simple tail implementation - read file, return last N lines
    try:
        with open(path, 'rb') as f:
            data = f.read()
    except Exception:
        return ''
    try:
        text = data.decode('utf-8', errors='replace')
    except Exception:
        text = data.decode('latin-1', errors='replace')
    all_lines = text.splitlines()
    return '\n'.join(all_lines[-lines:])


def is_safe_log(name):
    # prevent path traversal
    if '/' in name or '\\' in name:
        return False
    full = os.path.join(LOG_DIR, name)
    return os.path.isfile(full) and os.path.commonpath([os.path.abspath(full), os.path.abspath(LOG_DIR)]) == os.path.abspath(LOG_DIR)


@app.route('/')
def index():
    return render_template('control_v0.html')


@app.route('/api/logs')
def api_list_logs():
    return jsonify({'logs': safe_list_logs()})


@app.route('/api/logs/content')
def api_log_content():
    name = request.args.get('file')
    lines = int(request.args.get('lines', '200'))
    if not name or not is_safe_log(name):
        return jsonify({'error': 'invalid file'}), 400
    path = os.path.join(LOG_DIR, name)
    content = tail_lines(path, lines=lines)
    return jsonify({'file': name, 'lines': lines, 'content': content})


@app.route('/api/logs/download')
def api_log_download():
    name = request.args.get('file')
    if not name or not is_safe_log(name):
        return abort(404)
    return send_from_directory(LOG_DIR, name, as_attachment=True)


@app.route('/api/services')
def api_services():
    # Read pid files in log/ and report whether process is alive
    out = {}
    for fname in os.listdir(LOG_DIR) if os.path.isdir(LOG_DIR) else []:
        if fname.endswith('.pid'):
            try:
                with open(os.path.join(LOG_DIR, fname), 'r') as f:
                    pid = int(f.read().strip())
                alive = False
                try:
                    os.kill(pid, 0)
                    alive = True
                except Exception:
                    alive = False
                out[fname] = {'pid': pid, 'alive': alive}
            except Exception:
                out[fname] = {'error': 'could not read pid'}
    return jsonify({'services': out})


@app.route('/api/services/action', methods=['POST'])
def api_services_action():
    """Run start/stop/restart scripts. Returns JSON with status and output/pid.

    POST body (json): {"action": "start"|"stop"|"restart"}
    """
    data = request.get_json() or {}
    action = data.get('action')
    if action not in ('start', 'stop', 'restart', 'clear'):
        return jsonify({'error': 'invalid action'}), 400

    work_dir = os.getcwd()
    start_script = os.path.join(work_dir, 'start_services.sh')
    stop_script = os.path.join(work_dir, 'stop_services.sh')

    def exists_and_exec(path):
        return os.path.isfile(path) and os.access(path, os.X_OK)

    try:
        if action == 'start':
            if not exists_and_exec(start_script):
                return jsonify({'error': 'start script not found or not executable', 'path': start_script}), 404
            # run start in background and return pid
            # Use nohup so process persists; write wrapper log
            out_log = os.path.join(LOG_DIR, 'control_start.log')
            cmd = f"nohup bash {shlex.quote(start_script)} > {shlex.quote(out_log)} 2>&1 & echo $!"
            pid = subprocess.check_output(['/bin/bash', '-lc', cmd], cwd=work_dir).decode().strip()
            return jsonify({'status': 'started', 'pid': pid, 'log': out_log})

        if action == 'stop':
            if not exists_and_exec(stop_script):
                return jsonify({'error': 'stop script not found or not executable', 'path': stop_script}), 404
            out = subprocess.check_output(['/bin/bash', '-lc', f"bash {shlex.quote(stop_script)}"], cwd=work_dir, stderr=subprocess.STDOUT)
            return jsonify({'status': 'stopped', 'output': out.decode()})

        if action == 'restart':
            # run stop then start
            resp_stop = {}
            if exists_and_exec(stop_script):
                try:
                    out = subprocess.check_output(['/bin/bash', '-lc', f"bash {shlex.quote(stop_script)}"], cwd=work_dir, stderr=subprocess.STDOUT)
                    resp_stop['stopped'] = True
                    resp_stop['output'] = out.decode()
                except subprocess.CalledProcessError as e:
                    resp_stop['stopped'] = False
                    resp_stop['error'] = e.output.decode() if e.output else str(e)
            else:
                resp_stop['stopped'] = False
                resp_stop['error'] = 'stop script missing'

            resp_start = {}
            if exists_and_exec(start_script):
                out_log = os.path.join(LOG_DIR, 'control_start.log')
                cmd = f"nohup bash {shlex.quote(start_script)} > {shlex.quote(out_log)} 2>&1 & echo $!"
                pid = subprocess.check_output(['/bin/bash', '-lc', cmd], cwd=work_dir).decode().strip()
                resp_start = {'started': True, 'pid': pid, 'log': out_log}
            else:
                resp_start = {'started': False, 'error': 'start script missing'}

            return jsonify({'stop': resp_stop, 'start': resp_start})

        if action == 'clear':
            # Only allow clearing when no tracked services are alive
            alive_any = False
            details = {}
            for fname in os.listdir(LOG_DIR) if os.path.isdir(LOG_DIR) else []:
                if fname.endswith('.pid'):
                    try:
                        with open(os.path.join(LOG_DIR, fname), 'r') as f:
                            pid = int(f.read().strip())
                        try:
                            os.kill(pid, 0)
                            alive_any = True
                            details[fname] = {'pid': pid, 'alive': True}
                        except Exception:
                            details[fname] = {'pid': pid, 'alive': False}
                    except Exception:
                        details[fname] = {'error': 'could not read pid'}

            if alive_any:
                return jsonify({'error': 'cannot clear while services are running', 'services': details}), 400

            clean_script = os.path.join(work_dir, 'clean_data_output.sh')
            if not exists_and_exec(clean_script):
                return jsonify({'error': 'clean script not found or not executable', 'path': clean_script}), 404
            out = subprocess.check_output(['/bin/bash', '-lc', f"bash {shlex.quote(clean_script)}"], cwd=work_dir, stderr=subprocess.STDOUT)
            return jsonify({'status': 'cleared', 'output': out.decode()})

    except subprocess.CalledProcessError as e:
        return jsonify({'error': 'command failed', 'output': e.output.decode() if e.output else str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
