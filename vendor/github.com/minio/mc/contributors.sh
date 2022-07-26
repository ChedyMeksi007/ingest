#!/bin/bash
#
# Copyright (c) 2015-2021 MinIO, Inc.
#
# This file is part of MinIO Object Storage stack
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

set -e

cd "$(dirname "$(readlink -f "$BASH_SOURCE")")"

# see also ".mailmap" for how email addresses and names are deduplicated

{
	cat <<-'EOH'
## Contributors
<!-- DO NOT EDIT - CONTRIBUTORS.md is autogenerated from git commit log by contributors.sh script. -->
	EOH
	echo
	git log --format='%aN <%aE>' | LC_ALL=C.UTF-8 sort -uf | sed 's/^/- /g'
} > CONTRIBUTORS.md
