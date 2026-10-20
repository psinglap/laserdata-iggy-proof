#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import urllib.error
import urllib.request

from docker.types import Ulimit
from typing_extensions import Self

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready


class IggyContainer(DockerContainer):
    """
    Apache Iggy container: a persistent message streaming server.

    The server runs thread-per-core on io_uring, which Docker's default seccomp
    profile blocks, so the container starts with ``seccomp:unconfined``,
    ``SYS_NICE`` and an unlimited ``memlock``, as in Iggy's own compose file.

    Example:

        .. doctest::

            >>> import asyncio
            >>> from apache_iggy import IggyClient, SendMessage
            >>> from testcontainers.community.iggy import IggyContainer

            >>> async def send_one():
            ...     with IggyContainer() as iggy:
            ...         client = IggyClient.from_connection_string(iggy.get_connection_string())
            ...         await client.connect()
            ...         producer = await client.producer("demo", "events")
            ...         async with producer:
            ...             await producer.send_one(SendMessage("hello"))
            ...         return "sent"
            >>> asyncio.run(send_one())
            'sent'
    """

    TCP_PORT = 8090
    HTTP_PORT = 3000

    def __init__(
        self,
        image: str = "apache/iggy:0.9.0",
        username: str = "iggy",
        password: str = "iggy",
        ready_timeout_secs: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(image, **kwargs)
        self.username = username
        self.password = password
        # Without these the server creates the root user with a random password.
        self.with_env("IGGY_ROOT_USERNAME", username)
        self.with_env("IGGY_ROOT_PASSWORD", password)
        self._ready_timeout_secs = ready_timeout_secs
        self.with_exposed_ports(self.TCP_PORT, self.HTTP_PORT)
        self.with_kwargs(
            cap_add=["SYS_NICE"],
            security_opt=["seccomp:unconfined"],
            ulimits=[Ulimit(name="memlock", soft=-1, hard=-1)],
        )

    @wait_container_is_ready(urllib.error.URLError, ConnectionError, OSError)
    def _healthcheck(self) -> None:
        # /ping is Iggy's unauthenticated liveness route.
        with urllib.request.urlopen(f"{self.get_http_url()}/ping", timeout=2) as response:
            if response.status != 200:
                raise ConnectionError(f"Iggy /ping returned {response.status}")

    def get_tcp_host_and_port(self) -> tuple[str, int]:
        return self.get_container_host_ip(), int(self.get_exposed_port(self.TCP_PORT))

    def get_connection_string(self) -> str:
        """Connection string for Iggy's SDKs, for example ``iggy+tcp://iggy:iggy@localhost:49153``."""
        host, port = self.get_tcp_host_and_port()
        return f"iggy+tcp://{self.username}:{self.password}@{host}:{port}"

    def get_http_url(self) -> str:
        return f"http://{self.get_container_host_ip()}:{self.get_exposed_port(self.HTTP_PORT)}"

    def start(self) -> Self:
        super().start()
        self._healthcheck()
        return self
