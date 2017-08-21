use strict;
use warnings;
use Test::More tests => 7;
use Cwd;
use npg_tracking::util::abs_path qw(abs_path);

use_ok('npg_tracking::daemon::libmerge');
{
    my $r = npg_tracking::daemon::libmerge->new();
    isa_ok($r, 'npg_tracking::daemon::libmerge');
}

{
    my $runner  = q[npg_library_merging_runner];
    my $command = qq[$runner --sleep_time 21600];
    
    my $log_dir = abs_path(join(q[/],getcwd(), 'logs'));
    my $r = npg_tracking::daemon::libmerge->new(timestamp => 2016);
    is($r->command, $command, 'command to run');
    is($r->daemon_name, 'npg_library_merging_runner', 'default daemon name');
    my $host = q[sf-1-1-01];
    my $test = q{[[ -d } . $log_dir . q{ && -w } . $log_dir . q{ ]] && };
    my $error = q{ || echo Log directory } .  $log_dir . q{ for staging host } . $host . q{ cannot be written to};
    my $action = $test . qq[daemon -i -r -a 10 -n $runner --umask 002 -A 10 -L 10 -M 10 -o $log_dir/$runner-$host-2016.log -- $command] . $error;

    is($r->start($host), $action, 'start command');
    is($r->ping, q[daemon --running -n npg_library_merging_runner && ((if [ -w /tmp/npg_library_merging_runner.pid ]; then touch -mc /tmp/npg_library_merging_runner.pid; fi) && echo -n 'ok') || echo -n 'not ok'], 'ping command');
    is($r->stop, q[daemon --stop -n npg_library_merging_runner], 'stop command');
}

1;
