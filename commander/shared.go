package commander
import (
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
	"log"
	"encoding/json"
	"io/ioutil"


	"strconv"
	"syscall"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/litl/galaxy/commander"
	"github.com/litl/galaxy/config"
	"github.com/litl/galaxy/discovery"
	"github.com/litl/galaxy/log"
	"github.com/litl/galaxy/runtime"
	"github.com/litl/galaxy/utils"
	"github.com/litl/galaxy/config"

)

type dumpConfig struct {
	Pools   []string
	Hosts   []config.HostInfo
	Configs []config.AppDefinition
	Regs    []config.ServiceRegistration
}

var (
	stopCutoff     int64
	apps           []string
	env            string
	pool           string
	registryURL    string
	loop           bool
	hostIP         string
	dns            string
	shuttleAddr    string
	debug          bool
	runOnce        bool
	version        bool
	buildVersion   string
	configStore    *config.Store
	serviceRuntime *runtime.ServiceRuntime
	workerChans    map[string]chan string
	wg             sync.WaitGroup
	signalsChan    chan os.Signal
)

func initOrDie() {

	if registryURL == "" {
		log.Fatalf("ERROR: Registry URL not specified. Use '-registry redis://127.0.0.1:6379' or set 'GALAXY_REGISTRY_URL'")
	}

	configStore = config.NewStore(config.DefaultTTL, registryURL)

	serviceRuntime = runtime.NewServiceRuntime(configStore, dns, hostIP)

	apps, err := configStore.ListAssignments(env, pool)
	if err != nil {
		log.Fatalf("ERROR: Could not retrieve service configs for /%s/%s: %s", env, pool, err)
	}

	workerChans = make(map[string]chan string)
	for _, app := range apps {
		appCfg, err := configStore.GetApp(app, env)
		if err != nil {
			log.Fatalf("ERROR: Could not retrieve service config for /%s/%s: %s", env, pool, err)
		}

		workerChans[appCfg.Name()] = make(chan string)
	}

	signalsChan = make(chan os.Signal, 1)
	signal.Notify(signalsChan, os.Interrupt, os.Kill, syscall.SIGTERM)
	go deregisterHost(signalsChan)
}

func ensureEnv() {
	envs, err := configStore.ListEnvs()
	if err != nil {
		log.Fatalf("ERROR: Could not check envs: %s", err)
	}

	if strings.TrimSpace(env) == "" {
		log.Fatalf("ERROR: Need an env.  Use '-env <env>'. Existing envs are: %s.", strings.Join(envs, ","))
	}
}

func ensurePool() {

	pools, err := configStore.ListPools(env)
	if err != nil {
		log.Fatalf("ERROR: Could not check pools: %s", err)
	}

	if strings.TrimSpace(pool) == "" {
		log.Fatalf("ERROR: Need a pool.  Use '-pool <pool>'. Existing pools are: %s", strings.Join(pools, ","))
	}
}

func pullImageAsync(appCfg config.App, errChan chan error) {
	// err logged via pullImage
	_, err := pullImage(appCfg)
	if err != nil {
		errChan <- err
		return
	}
	errChan <- nil
}

func pullImage(appCfg config.App) (*docker.Image, error) {
	image, err := serviceRuntime.PullImage(appCfg.Version(), appCfg.VersionID())
	if image == nil || err != nil {
		log.Errorf("ERROR: Could not pull image %s: %s", appCfg.Version(), err)
		return nil, err
	}

	log.Printf("Pulled %s version %s\n", appCfg.Name(), appCfg.Version())
	return image, nil
}

func startService(appCfg config.App, logStatus bool) {

	desired, err := commander.Balanced(configStore, hostIP, appCfg.Name(), env, pool)
	if err != nil {
		log.Errorf("ERROR: Could not determine instance count: %s", err)
		return
	}

	running, err := serviceRuntime.InstanceCount(appCfg.Name(), strconv.FormatInt(appCfg.ID(), 10))
	if err != nil {
		log.Errorf("ERROR: Could not determine running instance count: %s", err)
		return
	}

	for i := 0; i < desired-running; i++ {
		container, err := serviceRuntime.Start(env, pool, appCfg)
		if err != nil {
			log.Errorf("ERROR: Could not start containers: %s", err)
			return
		}

		log.Printf("Started %s version %s as %s\n", appCfg.Name(), appCfg.Version(), container.ID[0:12])

		err = serviceRuntime.StopOldVersion(appCfg, 1)
		if err != nil {
			log.Errorf("ERROR: Could not stop containers: %s", err)
		}
	}

	running, err = serviceRuntime.InstanceCount(appCfg.Name(), strconv.FormatInt(appCfg.ID(), 10))
	if err != nil {
		log.Errorf("ERROR: Could not determine running instance count: %s", err)
		return
	}

	for i := 0; i < running-desired; i++ {
		err := serviceRuntime.Stop(appCfg)
		if err != nil {
			log.Errorf("ERROR: Could not stop container: %s", err)
		}
	}

	err = serviceRuntime.StopOldVersion(appCfg, -1)
	if err != nil {
		log.Errorf("ERROR: Could not stop old containers: %s", err)
	}

	// check the image version, and log any inconsistencies
	inspectImage(appCfg)
}

func heartbeatHost() {
	_, err := configStore.CreatePool(pool, env)
	if err != nil {
		log.Fatalf("ERROR: Unabled to create pool %s: %s", pool, err)
	}

	defer wg.Done()
	for {
		configStore.UpdateHost(env, pool, config.HostInfo{
			HostIP: hostIP,
		})

		time.Sleep(45 * time.Second)
	}
}

func deregisterHost(signals chan os.Signal) {
	<-signals
	configStore.DeleteHost(env, pool, config.HostInfo{
		HostIP: hostIP,
	})
	discovery.Unregister(serviceRuntime, configStore, env, pool, hostIP, shuttleAddr)
	os.Exit(0)
}

func appAssigned(app string) (bool, error) {
	assignments, err := configStore.ListAssignments(env, pool)
	if err != nil {
		return false, err
	}

	if !utils.StringInSlice(app, assignments) {
		return false, nil
	}
	return true, nil
}

// inspectImage checks that the running image matches the config.
// We only use this to print warnings, since we likely need to deploy a new
// config version to fix the inconsistency.
func inspectImage(appCfg config.App) {
	image, err := serviceRuntime.InspectImage(appCfg.Version())
	if err != nil {
		log.Println("error inspecting image", appCfg.Version())
		return
	}

	if utils.StripSHA(image.ID) != appCfg.VersionID() {
		log.Printf("warning: %s image ID does not match config", appCfg.Name())
	}
}

func restartContainers(app string, cmdChan chan string) {
	defer wg.Done()
	logOnce := true

	ticker := time.NewTicker(10 * time.Second)

	for {

		select {

		case cmd := <-cmdChan:

			assigned, err := appAssigned(app)
			if err != nil {
				log.Errorf("ERROR: Error retrieving assignments for %s: %s", app, err)
				if !loop {
					return
				}
				continue
			}

			if !assigned {
				continue
			}

			appCfg, err := configStore.GetApp(app, env)
			if err != nil {
				log.Errorf("ERROR: Error retrieving service config for %s: %s", app, err)
				if !loop {
					return
				}
				continue
			}

			if appCfg.Version() == "" {
				if !loop {
					return
				}
				continue
			}

			if cmd == "deploy" {
				_, err = pullImage(appCfg)
				if err != nil {
					log.Errorf("ERROR: Error pulling image for %s: %s", app, err)
					if !loop {
						return
					}
					continue
				}
				startService(appCfg, logOnce)
			}

			if cmd == "restart" {
				err := serviceRuntime.Stop(appCfg)
				if err != nil {
					log.Errorf("ERROR: Could not stop %s: %s",
						appCfg.Version(), err)
					if !loop {
						return
					}

					startService(appCfg, logOnce)
					continue
				}
			}

			logOnce = false
		case <-ticker.C:

			appCfg, err := configStore.GetApp(app, env)
			if err != nil {
				log.Errorf("ERROR: Error retrieving service config for %s: %s", app, err)
				continue
			}

			assigned, err := appAssigned(app)
			if err != nil {
				log.Errorf("ERROR: Error retrieving service config for %s: %s", app, err)
				if !loop {
					return
				}

				continue
			}

			if appCfg == nil || !assigned {
				log.Errorf("%s no longer exists.  Stopping worker.", app)
				serviceRuntime.StopAllMatching(app)
				delete(workerChans, app)
				return
			}

			if appCfg.Version() == "" {
				continue
			}

			startService(appCfg, logOnce)
		}

		if !loop {
			return
		}

	}
}

func monitorService(changedConfigs chan *config.ConfigChange) {

	for {

		var changedConfig *config.ConfigChange
		select {

		case changedConfig = <-changedConfigs:

			if changedConfig.Error != nil {
				log.Errorf("ERROR: Error watching changes: %s", changedConfig.Error)
				continue
			}

			if changedConfig.AppConfig == nil {
				continue
			}

			assigned, err := appAssigned(changedConfig.AppConfig.Name())
			if err != nil {
				log.Errorf("ERROR: Error retrieving service config for %s: %s", changedConfig.AppConfig.Name(), err)
				if !loop {
					return
				}
				continue
			}

			if !assigned {
				continue
			}

			ch, ok := workerChans[changedConfig.AppConfig.Name()]
			if !ok {
				name := changedConfig.AppConfig.Name()
				ch := make(chan string)
				workerChans[name] = ch
				wg.Add(1)
				go restartContainers(name, ch)
				ch <- "deploy"

				log.Printf("Started new worker for %s\n", name)
				continue
			}

			if changedConfig.Restart {
				log.Printf("Restarting %s", changedConfig.AppConfig.Name())
				ch <- "restart"
			} else {
				ch <- "deploy"
			}
		}
	}

}

type dumpConfig struct {
	Pools   []string
	Hosts   []config.HostInfo
	Configs []config.AppDefinition
	Regs    []config.ServiceRegistration
}

// Dump everything related to a single environment from galaxy to stdout,
// including current runtime config, hosts, IPs etc.
// This isn't really useful other than to sync between config backends, but we
// can probably convert this to a better backup once we stabilize the code some
// more.
func dump(env string) {
	envDump := &dumpConfig{
		Configs: []config.AppDefinition{},
		Regs:    []config.ServiceRegistration{},
	}

	pools, err := configStore.ListPools(env)
	if err != nil {
		log.Fatal(err)
	}

	envDump.Pools = pools

	for _, pool := range pools {
		hosts, err := configStore.ListHosts(env, pool)
		if err != nil {
			log.Fatal(err)
		}
		for _, host := range hosts {
			host.Pool = pool
			envDump.Hosts = append(envDump.Hosts, host)
		}
	}

	apps, err := configStore.ListApps(env)
	if err != nil {
		log.Fatal(err)
	}

	for _, app := range apps {
		// AppDefinition is intended to be serializable itself
		if ad, ok := app.(*config.AppDefinition); ok {
			envDump.Configs = append(envDump.Configs, *ad)
			continue
		}

		// otherwise, manually convert the App to an AppDefinition
		ad := config.AppDefinition{
			AppName:     app.Name(),
			Image:       app.Version(),
			ImageID:     app.VersionID(),
			Environment: app.Env(),
		}

		for _, pool := range app.RuntimePools() {
			ad.SetProcesses(pool, app.GetProcesses(pool))
			ad.SetMemory(pool, app.GetMemory(pool))
			ad.SetCPUShares(pool, app.GetCPUShares(pool))
		}

		envDump.Configs = append(envDump.Configs, ad)
	}

	// The registrations are temporary, but dump them anyway, so we can try and
	// convert an environment by keeping the runtime config in sync.
	regs, err := configStore.ListRegistrations(env)
	if err != nil {
		log.Fatal(err)
	}
	envDump.Regs = append(envDump.Regs, regs...)

	js, err := json.MarshalIndent(envDump, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	os.Stdout.Write(js)
}

// Restore everything we can from a Galaxy dump on stdin.
// This probably will panic if not using consul
func restore(env string) {
	js, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}

	envDump := dumpConfig{}
	err = json.Unmarshal(js, &envDump)
	if err != nil {
		log.Fatal(err)
	}

	for _, pool := range envDump.Pools {
		_, err := configStore.CreatePool(env, pool)
		if err != nil {
			log.Println(err)
		}
	}

	for _, appDef := range envDump.Configs {
		_, err := configStore.UpdateApp(&appDef, env)
		if err != nil {
			log.Println(err)
		}
	}

	for _, hostInfo := range envDump.Hosts {
		err := configStore.UpdateHost(env, pool, hostInfo)
		if err != nil {
			log.Println(err)
		}
	}

	for _, reg := range envDump.Regs {
		err := configStore.Backend.RegisterService(env, reg.Pool, &reg)
		if err != nil {
			log.Println(err)
		}
	}

}
