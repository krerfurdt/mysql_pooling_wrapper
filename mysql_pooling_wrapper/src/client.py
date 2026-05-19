import os
import platform
import subprocess
from mysql.connector import pooling, Error
from typing import List, Any
from contextlib import contextmanager
from logging import getLogger as logger

class client:
    def __init__(self, **kwargs: Any):
        self.cfg = {}
        self.error = -1
        self.log = logger('mysql_pooling_wrapper.client')
        if 'debug' in kwargs and kwargs['debug'] == True:
            self.log._set_debug(True)
        self._check_connection_args(**kwargs)
        self._check_mysql_args(**kwargs)
        try:
            self.pool = pooling.MySQLConnectionPool(**self.cfg)
        except Exception as e:
            self.log.exception(f'Failed to init DB pool: {e}')
            raise
        except Error as err:
            self.log.error(f'Failed to init DB pool: {err}')
            raise

    def _check_connection_args(self, **kwargs):
        try:
            if platform.system().lower() == 'linux':
                if 'unix_socket' not in kwargs:
                    socket_paths = ["/var/run/mysqld/mysqld.sock", "/tmp/mysql.sock", "/var/lib/mysql/mysql.sock"]
                    found_path = next((path for path in socket_paths if os.path.exists(path)), None)
                    if not found_path:
                        try:
                            cmd = ["find", "/run", "/var", "/tmp", "-name", "mysqld.sock", "-type", "s"]
                            search_res = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
                            paths = search_res.stdout.strip().split('\n')
                            if paths and os.path.exists(paths[0]):
                                found_path = paths[0]
                        except (subprocess.SubprocessError, Exception):
                            pass
                    if found_path is not None:
                        self.cfg['unix_socket'] = found_path
                    else:
                        self.cfg['host'] = kwargs.get('host', '127.0.0.1')
                        self.cfg['port'] = int(kwargs.get('port', 3306))
            else:
                self.cfg['host'] = kwargs.get('host', '127.0.0.1')
                self.cfg['port'] = int(kwargs.get('port', 3306))
        except Exception as e:
            self.log.exception(f'Exception during connection configuration: {e}')
    
    def _check_mysql_args(self, **kwargs):
        self.safe_dbs = kwargs.get('safe_dbs', [])
        self.safe_tables = kwargs.get('safe_tables', [])
        self.cfg['database'] = kwargs.get('database')
        self.cfg['user'] = kwargs.get('user')
        self.cfg['password'] = kwargs.get('password')
        self.cfg["pool_name"] = kwargs.get('pool_name', f"{self.__class__.__name__}.{kwargs.get('database', __name__)}.pool")
        self.cfg["pool_size"] = int(kwargs.get('pool_size', 3))
        self.cfg["pool_reset_session"] = bool(kwargs.get('pool_reset_session', True))
        self.cfg["charset"] = kwargs.get('charset', 'utf8mb4')
        self.cfg["connect_timeout"] = kwargs.get('connect_timeout', 10)
        self.cfg["consume_results"] = kwargs.get('consume_results', True)
        self.cfg["raise_on_warnings"] = kwargs.get('raise_on_warnings', True)
        self.cfg["autocommit"] = kwargs.get('autocommit', False)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        destroy = getattr(self.pool, "destroy", None)
        if callable(destroy):
            destroy()
        return False        

    @contextmanager
    def connection(self):
        conn = None
        try:
            conn = self.pool.get_connection()
            yield conn
        except Exception as e:
            self.log.exception(f'Connection failed: {e}')
            if conn: conn.rollback()
            raise
        except Error as err:
            self.log.error(f'Connection failed: {err}')
            if conn: conn.rollback()
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()

    def execute(self, sql: str, params: tuple = None) -> int:
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params if params is not None else ())
                    count = cursor.rowcount
                    conn.commit()
                return count
        except Exception as e:
            self.log.exception(f'Execute failed: {e}')
            return self.error
        except Error as err:
            self.log.error(f'Execute failed: {err}')
            return self.error

    def executemany(self, sql: str, params: List[tuple]) -> int:
        if not params:
            self.log.debug("No parameters provided to executemany; skipping")
            return 0
        try:
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    count = cursor.executemany(sql, params)
                    conn.commit()
                return count
        except Exception as e:
            self.log.exception(f'Execute failed: {e}')
            return self.error
        except Error as err:
            self.log.error(f'Execute failed: {err}')
            return self.error

    def query(self, sql: str, params: tuple = None, one: bool = False) -> Any:
        try:
            with self.connection() as conn:
                with conn.cursor(dictionary=True) as cursor:
                    cursor.execute(sql, params if params is not None else ())
                    return cursor.fetchone() if one else cursor.fetchall()
        except Exception as e:
            self.log.exception(f'Query failed: {e}')
            return None
        except Error as err:
            self.log.error(f'Query failed: {err}')
            return self.error
